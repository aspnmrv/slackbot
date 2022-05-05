import sql_functions
import os
import pandas as pd
import json

from worker import Worker
from adm.admin import get_data_db, get_text_from_request, get_interactivity, get_user_from_request, update_data_db, get_text_interactivity, send_message_with_url
from config import IDS_USERS_PERMISSIONS_UPDATE
from adm.verify import verify
from typing import List


def get_items(request) -> pd.DataFrame:
	"""The function reads parameters and returns a file with products"""

	text = get_text_from_request(request)
	items = text.split("\n")
	return get_data_db(sql_functions.sql_get_name_of_items(items)) if len(items) > 1 else pd.DataFrame()


elem = {
	"blocks": [
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "I sent you a file, check the correctness of the downloaded SKUs. Update?"
			},
		},
		{
			"type": "actions",
			"block_id": "button_pvcore_update",
			"elements": [
				{
					"type": "button",
					"text": {
						"type": "plain_text",
						"text": "Confirm"
					},
					"style": "primary",
					"value": "success",
					"action_id": "pvcore_update_confirm"
				},
				{
					"type": "button",
					"text": {
						"type": "plain_text",
						"text": "No, cancel"
					},
					"style": "danger",
					"value": "cancel",
					"action_id": "pvcore_update_cancel"
				}
			]
		}
	]
}


def send_files_update_items(old_items: List[int], new_items: List[int]) -> pd.DataFrame:
	remove_items = list(set(old_items) - set(new_items))
	added_items = list(set(new_items) - set(old_items))

	df = get_data_db(sql_functions.sql_get_items())
	df_removed = df[df["id"].isin(list(remove_items))]
	df_added = df[df["id"].isin(list(added_items))]

	df_removed["action"] = "remove"
	df_added["action"] = "add"

	df_result = pd.concat([df_removed, df_added])
	return df_result



class UpdateItemsPvCore(Worker):
	def __init__(self, sender, channel_id):
		super().__init__(sender, channel_id)

	def update_items_pv_core(self, request, response_url):
		"""The function updates the products in the table data.core_products"""

		file = "df_items_pv_core.xlsx"
		answer = json.loads(request.form.get('payload')).get('actions')[0].get('value')
		if answer == "success":
			df = pd.read_excel(file)
			os.remove(file)
			df["id"] = df["id"].astype(str)
			items = ",".join(df.id)
			old_items = get_data_db(sql_functions.sql_get_pv_core_items())
			#remove exist items
			update_data_db(sql_functions.sql_remove_items_from_pv_core())
			update_data_db(sql_functions.sql_update_pv_core_items(items))
			new_items = get_data_db(sql_functions.sql_get_pv_core_items())
			if set(list(old_items.id_product)) == set(list(new_items.id_product)):
				send_message_with_url(response_url, "❌ Most likely, you are sending the same list "
													"of products that already exists. \n"
													"Or the list has not been updated for technical reasons. \n"
													"You can check here: https://redash.com/queries/309")
			else:
				send_message_with_url(response_url, "✅ The list has been successfully updated, but it's "
													"better to check here: "
													"https://redash.com/queries/309 \n"
													"Contact again!")

				df_check = send_files_update_items(list(old_items.id_product), list(new_items.id_product))
				if df_check.shape[0] > 0:
					df_check.to_excel("filename.xlsx", index=False)
					self.sender.send_files("filename.xlsx", self.channel_id, "filename.xlsx", "filename")
					os.remove("filename.xlsx")
		else:
			send_message_with_url(response_url, "📛 I understand. I don't update anything!")
			os.remove(file)
		return '', 200

	def get_job_pv_core(self, request):
		print(verify(request))
		"""The function sends a file with new products for verification"""
		user_id = get_user_from_request(request)
		if user_id in (IDS_USERS_PERMISSIONS_UPDATE):
			df_items = get_items(request)
			if df_items.shape[0] == 0:
				return get_interactivity(request, {
					"text": "You have not entered parameters..."
				})
			else:
				file = "filename.xlsx"
				df_items.to_excel(file, index=False)
				self.sender.send_files(file, user_id, file, file)
				return get_interactivity(request, elem)
		else:
			return get_text_interactivity(request, {
    			"text": "You don't have access 🙁"
			})
