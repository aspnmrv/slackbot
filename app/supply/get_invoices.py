import sql_functions
import os

from worker import Worker
from adm.admin import get_data_db
from datetime import datetime


class GetInvoices(Worker):
    def __init__(self, sender, channel_id):
        super().__init__(sender, channel_id)

    def get_invoices(self, datefrom, dateto, user_id):
        if datetime.strptime(dateto, "%Y-%m-%d") > datetime.today():
            self.sender.send_message(user_id, "Последняя дата не может быть больше текущей")
        elif datetime.strptime(datefrom, "%Y-%m-%d") > datetime.strptime(dateto, "%Y-%m-%d"):
            self.sender.send_message(user_id, "Неправильный формат дат")
        else:
            data = get_data_db(sql_functions.sql_invoices_test(datefrom, dateto))
            data.to_excel(f"filename{datefrom}-{dateto}.xlsx")
            file = f"filename{datefrom}-{dateto}.xlsx"
            self.sender.send_files(file, user_id, file, file)
            os.remove(file)
            return '', 200
