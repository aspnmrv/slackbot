import random
import sql_functions
from globals import date_dict, list_hello
from worker import Worker
from adm.admin import get_data_db


class Supply(Worker):
    def __init__(self, sender, channel_id):
        super().__init__(sender, channel_id)

    def send(self):
        pv = get_data_db(sql_functions.sql_pv_test(date_dict['dateFrom'], date_dict['dateTo'])).iloc[0, 0]
        pv_ab = get_data_db(sql_functions.sql_pv_ab_test(date_dict['dateFrom'], date_dict['dateTo'])).iloc[0, 0]
        psb_pv = round(get_data_db(sql_functions.sql_psb(date_dict['dateFrom'], date_dict['dateTo'])).iloc[0, 0] - pv, 2)
        pv_core = get_data_db(sql_functions.sql_pv_core(date_dict['dateFrom'], date_dict['dateTo'])).iloc[0, 0]

        hello_text = random.choice(list_hello) + " 📊️ "

        text = hello_text + "\n" + "\n" + "* ----- PV / PV AB / PSB / PV Core -----* " + "\n" + "\n" \
            f"Yesterday PV: `{pv} %`" + "\n" + f"Yesterday PV AB: `{pv_ab} %`" + "\n" \
            + f"Yesterday PSB - PV: `{psb_pv} %`" + "\n" + f"Yesterday PV Core: `{pv_core} %`" + "\n"

        self.sender.send_message(self.channel_id, text)
