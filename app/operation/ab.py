from worker import Worker
from globals import date_dict
import os
import sql_functions
import random


class AB(Worker):
    def __init__(self, sender, channel_id):
        super().__init__(sender, channel_id)

    def send(self):
        pv = sql_functions.sql_pv(date_dict['dateFrom'], date_dict['dateTo'])
        pv_ab = sql_functions.sql_pv_ab(date_dict['dateFrom'], date_dict['dateTo'])
        sql_functions.sql_graphs_ab(date_dict['dateFromBQ2'], date_dict['dateToBQ'])
        sql_functions.sql_graphs_ab_by_days(date_dict['dateToBQ'], date_dict['dateToBQ'])

        if date_dict['dow'] == 6 or date_dict['dow'] == 5:
            list_general_hello = ["Привет! Отличных выходных!",
                                  ]
        else:
            list_general_hello = ["Всем привет! Я к вам с интересными данными...",
                                  "Всем привет! Отличного дня!",
                                  ]

        hello_text = random.choice(list_general_hello) + " :sunny: "

        text = hello_text + "\n" + "PV -------------- " + "\n" + "*PV вчера:*  *{0} % * ".format(pv) + "\n" \
                                                                                                       "" + "\n" + "*PV AB вчера:*  *{0} % * ".format(
            pv_ab) + "\n" + "-------------" + "\n" + \
               "*Динамика показов АБ-алертов в графиках:* " + "\n"

        self.sender.send_message(channel=self.channel_id, text=text)

        #
        self.sender.send_files(files=f'filename_ab{date_dict["date_now"]}.png',
                               channels=self.channel_id,
                               filename=f'filename_ab{date_dict["date_now"]}.png',
                               title=f'filename_ab{date_dict["date_now"]}.png'
                               )

        self.sender.send_files(files=f'filename_ab_hour{date_dict["date_now"]}.png',
                               channels=self.channel_id,
                               filename=f'filename_ab_hour{date_dict["date_now"]}.png',
                               title=f'filename_ab_hour{date_dict["date_now"]}.png'
                               )

        os.remove(f'filename_ab{date_dict["date_now"]}.png')
        os.remove(f'filename_ab_hour{date_dict["date_now"]}.png')

