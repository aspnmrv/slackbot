import random
import sql_functions
from globals import date_dict, list_hello
from worker import Worker
from adm.admin import get_data_db


class General(Worker):
    def __init__(self, sender, channel_id):
        super().__init__(sender, channel_id)

    def send(self):
        order_total = get_data_db(sql_functions.sql_count_total_requests_test(date_dict['dateTo'])).iloc[0, 0]
        orders_yesterday = get_data_db(
            sql_functions.sql_count_requests_test(date_dict['dateFrom'], date_dict['dateTo'])).iloc[0, 0]
        new_users_yesterday = get_data_db(
            sql_functions.sql_new_users_test(date_dict['dateFrom'], date_dict['dateTo'])).iloc[0, 0]

        retention_month = round(get_data_db(sql_functions.sql_retention_month(date_dict['dateTo'])).iloc[0, 0], 1)
        aov_month = round(get_data_db(sql_functions.sql_aov_month(date_dict['dateTo'])).iloc[0, 0], 1)
        aar_month = int(get_data_db(sql_functions.sql_aar_month(date_dict['dateTo'])).iloc[0, 0])
        aar_month = f"{aar_month: ,}".replace(',', ' ')

        aov_yesterday = round(get_data_db(sql_functions.sql_aov(date_dict['dateFrom'],
                                                                date_dict['dateTo'])).iloc[0, 0], 1)
        gmv_yesterday = int(get_data_db(sql_functions.sql_gmv(date_dict['dateFrom'],
                                                                date_dict['dateTo'])).iloc[0, 0])
        pv = get_data_db(sql_functions.sql_pv_test(date_dict['dateFrom'], date_dict['dateTo'])).iloc[0, 0]
        pv_core = get_data_db(sql_functions.sql_pv_core(date_dict['dateFrom'], date_dict['dateTo'])).iloc[0, 0]

        hello_text = random.choice(list_hello) + " :sunny: "

        if orders_yesterday == 0:
            orders_yesterday = "Day-Off"

        text = hello_text + "\n" + "\n" + "*--Top Key Results till 2030--* " + "\n" + "\n" \
            "*1 Month Retention:* `40 %`" + "\n" + "*Average monthly AOV:* `100 €`" \
            + "\n" + "*Annual Run-Rate:* `99 500 000 €`" + "\n" + "\n" + "*---------Current situation--------*" + "\n" \
            + f"1 Month Retention: `{retention_month} %`" + "\n" + f"Average monthly AOV: `{aov_month} €`" + "\n" \
            + f"Annual Run-Rate: `{aar_month} €`" + "\n" + "\n" + "*-----------Base metrics----------*" + "\n" \
            + f"Total Orders: `{order_total}`" + "\n" + f"Yesterday Orders: `{orders_yesterday}`" + "\n" \
            + f"Yesterday New Users: `{new_users_yesterday}`" + "\n" + "\n" + f"Yesterday GMV: `{gmv_yesterday} €`" + "\n" \
            + f"Yesterday AOV: `{aov_yesterday} €`" + "\n" + "\n" + f"Yesterday PV: `{pv} %`" \
            + "\n" + f"Yesterday PV Core: `{pv_core} %`" + "\n"

        self.sender.send_message(self.channel_id, text)
