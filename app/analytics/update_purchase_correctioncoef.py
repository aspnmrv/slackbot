import os
import time

from worker import Worker
from adm.redash import get_result
from globals import NOTIFY_COEFS
from config import API_KEY
from adm.admin import update_data_db


class UpdatePurchaseCorrectionCoef(Worker):
    def __init__(self, sender, channel_id, notify=NOTIFY_COEFS):
        super().__init__(sender, channel_id)
        self.notify = notify


    def send(self):
        # df - выполнение скрипта для обновления коэффициентов
        # old_coeffs / new_coeffs - для каждого продукта его коэф до обновления и после обноваления
        # Если различий нет (коэф никак не изменились), значит, вероятно, они не обновились -> проблема
        df = get_result(258, {}, API_KEY)
        old_coeffs = get_result(259, {}, API_KEY)
        start_time = time.time()
        update_data_db(" ".join(df.request))
        update_time = ("--- %s seconds ---" % (time.time() - start_time))
        new_coeffs = get_result(259, {}, API_KEY)
        df_compare = old_coeffs.merge(new_coeffs, on=["id_product"], how="inner").dropna()
        df_compare["compare"] = df_compare["purchase_correctioncoef_x"] == df_compare["purchase_correctioncoef_y"]
        file_name = "filename.xlsx"
        df_compare.to_excel(file_name, index=False)
        count_not_update_coeffs = df_compare["compare"].values.sum()
        count_update_coeffs = (~df_compare["compare"]).values.sum()
        text = f"Update: {count_update_coeffs}, without {count_not_update_coeffs}, time: {update_time} ✅"
        if count_not_update_coeffs == 0:
            text = "Problem!‼️"

        self.sender.send_message(self.channel_id, text)
        self.sender.send_files(file_name, self.channel_id, file_name, "filename")
        os.remove(file_name)
        return '', 200
