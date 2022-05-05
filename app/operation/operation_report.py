from worker import Worker
from adm.redash import get_result
from globals import *
from config import *


class OperationReport(Worker):
    def __init__(self, sender, channel_id):
        super().__init__(sender, channel_id)

    def send(self):
        all_orders = get_result(269, {"date": {"start": date_dict['dateFrom'],
                                      "end": date_dict['dateTo']}}, API_KEY)
        if all_orders.shape[0] == 0:
            return
        else:
            problem_goods = get_result(271,
                                       {"date": {"start": date_dict['dateFrom'],
                                        "end": date_dict['dateTo']}}, API_KEY)
            delivery_delays = get_result(272,
                                         {"date": {"start": date_dict['dateFrom'],
                                          "end": date_dict['dateTo']}}, API_KEY)
            packing_delays = get_result(270,
                                        {"date": {"start": date_dict['dateFrom'],
                                         "end": date_dict['dateTo']}}, API_KEY)
            nps = get_result(274, {"date": {"start": date_dict['dateFrom'],
                                   "end": date_dict['dateTo']}}, API_KEY)

            text = '*Target indicators. Reporting period - 1 day.*' + "\n" +  \
                   "\n" + '*Total number of orders :*' + "\n" + \
                   "\n".join(['--- `{}: {}% ({})`'
                              .format(row['ds_name'],
                                      row['share_of_all_orders'],
                                      row['orders_number']) for
                              index, row in all_orders.iterrows()]) + "\n" + \
                   "\n" + '*Problem products*' + "\n" + \
                   "\n".join(['--- `{}: {}`'
                              .format(row['name_ds'],
                                      row['count_problems']) for index, row in
                              problem_goods.iterrows()]) + "\n" + \
                   "\n" + '*Delivery delays*' + "\n" + \
                   "\n".join(['--- `{}: {}% ({})`'
                              .format(row['name'],
                                      round(row['share_of_delays'], 2),
                                      row['orders']) for
                              index, row in delivery_delays.iterrows()]) + "\n" + \
                   "\n" + '*Packing delays*' + "\n" + \
                   "\n".join(['--- `{}: {}`'
                              .format(row['name_ds'],
                                      row['delay_orders']) for index, row in
                              packing_delays.iterrows()]) + "\n" + "\n" + \
                   '*Customer satisfaction (NPS Grosery) (High/Total)*: *{}* %' \
                   .format(round(nps['share_orders_high'][0], 2)) + "\n" + \
                   '-- Total:' + "\n" + \
                   '`High: {}`'.format(nps['orders_high'].sum()) + "\n" + \
                   '`Medium: {}`'.format(nps['orders_medium'].sum()) + "\n" + \
                   '`Low: {}`'.format(nps['orders_bad'].sum()) + "\n" + "\n" + \
                   "\n".join(['--- {}:\n`High: {}`\n`Medium: {}`\n`Low: {}`'
                              .format(row['name_ds'],
                                      row['orders_high'],
                                      row['orders_medium'],
                                      row['orders_bad'])
                              for index, row in nps.iterrows()]) + "\n" + \
                   "---------"

            self.sender.send_message(self.channel_id, text)
