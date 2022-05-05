from worker import Worker
from globals import *
import sql_functions


class DeleteProducts(Worker):
    def __init__(self, sender, channel_id):
        super().__init__(sender, channel_id)

    def send(self):
        df_delete_products = sql_functions.sql_delete_products()
        if len(df_delete_products) < 1:
            # pass
            self.sender.send_message(channel=self.channel_id, text="delete_products")
        else:
            text = "Доброе утро!" + \
                   "\n" + "Сегодня, {0}, есть продукты, которые нужно убрать из " \
                          "новинок :point_down: ".format(date_dict['today'])
            text_add = ""
            for i in range(len(df_delete_products)):
                id = df_delete_products.iloc[i, 0]
                name = df_delete_products.iloc[i, 1]
                start_date = df_delete_products.iloc[i, 2]
                date_ = start_date.strftime('%d/%m/%Y')
                text_add += f" —Продукт id = {id} - *{name}*, старт продаж был {date_}  \n"
            self.sender.send_message(channel=self.channel_id, text=text + "\n" + text_add)
