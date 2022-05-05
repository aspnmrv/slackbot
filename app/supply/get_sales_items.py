import sql_functions
import os

from worker import Worker
from adm.admin import get_data_db
from datetime import datetime, timedelta
from adm import admin


class GetSalesItems(Worker):
    def __init__(self, sender, channel_id):
        super().__init__(sender, channel_id)

    def get_sales_items(self, datefrom, dateto, user_id):
        if datetime.strptime(dateto, "%Y-%m-%d") > datetime.today():
            self.sender.send_message(user_id, "Последняя дата не может быть больше текущей")
        elif datetime.strptime(datefrom, "%Y-%m-%d") > datetime.strptime(dateto, "%Y-%m-%d"):
            self.sender.send_message(user_id, "Неправильный формат дат")
        else:
            datefrom = (datetime.strptime(datefrom, "%Y-%m-%d") +
                       timedelta(hours=-admin.get_value_delta(id_region=2))).strftime("%Y-%m-%d %H:00:00")
            dateto = (datetime.strptime(dateto, "%Y-%m-%d") +
                       timedelta(hours=24-admin.get_value_delta(id_region=2))).strftime("%Y-%m-%d %H:00:00")
            data = get_data_db(sql_functions.sql_sales_items_test(datefrom, dateto, 2))
            data.to_excel(f'filename{datefrom}-{dateto}.xlsx')
            file = f'filename{datefrom}-{dateto}.xlsx'
            self.sender.send_files(file, user_id, file, file)
            os.remove(file)
        return '', 200
