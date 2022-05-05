import redis
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import sql_functions
import os

from worker import Worker
from adm.admin import get_data_db
from globals import date_dict
from config import REDIS_HOST, REDIS_PORT
from typing import List

import matplotlib
matplotlib.use('agg')


def get_plot(df: pd.DataFrame, filename: str):
    """The function builds a supply shortage graph for the input dataframe"""
    sns.set_style("dark")
    sns.set_context({"figure.figsize": (28, 18)})

    main_graph = sns.barplot(x=df.datedepartured, y=df.required_count, color="red", ci=None)
    bottom_plot = sns.barplot(x=df.datedepartured, y=df["count"], color="#A9A9A9", ci=None)

    topbar = plt.Rectangle((0, 0), 1, 1, fc="red")
    bottombar = plt.Rectangle((0, 0), 1, 1, fc='#A9A9A9')
    l = plt.legend([bottombar, topbar], ['Supply', 'Underdelivery'], loc=1, ncol=2, prop={'size': 17})

    bottom_plot.set_ylabel("Share Count, %", fontweight='bold', size=40)
    bottom_plot.set_xlabel("Time (departured)", fontweight='bold', size=40)
    plt.xticks(rotation=0)
    plt.title("Undersending HUB-DS", size=55)

    for item in ([bottom_plot.xaxis.label, bottom_plot.yaxis.label] +
                 bottom_plot.get_xticklabels() + bottom_plot.get_yticklabels()):
        item.set_fontsize(19)
    plt.savefig(filename)
    plt.clf()
    plt.close()
    return


def create_non_completed_supplies(supplies_list: List[int], filename: str, datefrom: str, dateto: str):
    df_error = get_data_db(sql_functions.sql_get_data_for_plot_send(datefrom, dateto))
    if df_error.shape[0] > 0:
        df_error = df_error[df_error["id_storepointsupplies"].isin(supplies_list)]
        df_error.to_excel(filename, index=False)
    return


REDIS_POOL = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
r = redis.Redis(connection_pool=REDIS_POOL)


def get_data(key: str, last_key: str, expire_key: int, datefrom: str, dateto: str) -> pd.DataFrame:
    """The function returns a dataframe if there are new shipments, else None"""
    df_supplies = get_data_db(sql_functions.sql_get_new_supplies(datefrom, dateto))
    if df_supplies.shape[0] > 0:
        supplies_list = list(df_supplies.id)
    else:
        return None
    if len(supplies_list) > 0:
        if r.exists(key):
            if r.exists(last_key):
                current_supplies = [int(id_supply) for id_supply in r.smembers(key)] + \
                                   [int(id_supply) for id_supply in r.smembers(last_key)]
            else:
                current_supplies = [int(id_supply) for id_supply in r.smembers(key)]
            new_supplies_list = list(set(supplies_list) - set(current_supplies))
            if len(new_supplies_list) != 0:
                df = get_data_db(sql_functions.sql_get_data_for_plot(datefrom, dateto))
                df.datedepartured = pd.to_datetime(df.datedepartured).dt.strftime('%Y-%m-%d %H:%M')
                df = df[df["id_storepointsupplies"].isin(new_supplies_list)]
                if df.shape[0] > 0:
                    create_non_completed_supplies(new_supplies_list,
                                                  "df_error_{}.xlsx".format(str(df.iloc[0, 1])), datefrom, dateto)
                r.sadd(key, *new_supplies_list)
                r.expire(key, expire_key)
                return df
            else:
                return None
        elif r.exists(last_key):
                current_supplies = [int(id_supply) for id_supply in r.smembers(last_key)]
                new_supplies_list = list(set(supplies_list) - set(current_supplies))
                if len(new_supplies_list) != 0:
                    df = get_data_db(sql_functions.sql_get_data_for_plot(datefrom, dateto))
                    df.datedepartured = pd.to_datetime(df.datedepartured).dt.strftime('%Y-%m-%d %H:%M')
                    df = df[df["id_storepointsupplies"].isin(new_supplies_list)]
                    if df.shape[0] > 0:
                        create_non_completed_supplies(new_supplies_list,
                                                      "df_error_{}.xlsx".format(str(df.iloc[0, 1])), datefrom, dateto)
                    r.sadd(key, *new_supplies_list)
                    r.expire(key, expire_key)
                    return df
        else:
            df = get_data_db(sql_functions.sql_get_data_for_plot(datefrom, dateto))
            df.datedepartured = pd.to_datetime(df.datedepartured).dt.strftime('%Y-%m-%d %H:%M')
            df = df[df["id_storepointsupplies"].isin(supplies_list)]
            if df.shape[0] > 0:
                create_non_completed_supplies(supplies_list,
                                              "df_error_{}.xlsx".format(str(df.iloc[0, 1])), datefrom, dateto)
            r.sadd(key, *supplies_list)
            r.expire(key, expire_key)
            return df

    else:
        return None


class GetDataSupplyHubDs(Worker):
    def __init__(self, sender, channel_id):
        super().__init__(sender, channel_id)

    def send(self):
        filename = "df_hub_rc.png"
        stoday = (datetime.date.today()).isoformat()
        syesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        redis_key = "SLACKBOT:" + "id_supplies" + stoday
        redis_key_last = "SLACKBOT:" + "id_supplies" + syesterday
        df = get_data(redis_key, redis_key_last, 259200, date_dict['dateTo'], date_dict['current_date'])
        print(df)
        if df is not None and df.shape[0] > 0:
            filename_error = "df_error_{}.xlsx".format(str(df.iloc[0, 1]))
            text = "Required: {} \n Actual Supply: {} \n".format(
                round(df["unique_items_required"].sum(), 1),
                round(df["unique_items_fact"].sum(), 1)
            ) + "\n" + "Deviation from the schedule: {} minutes".format(int(df["diff_time"].sum()))
            get_plot(df, filename)
            self.sender.send_message(self.channel_id, text)
            self.sender.send_files(filename, self.channel_id, filename, "Undersending HUB-DS")
            os.remove(filename)
            if os.path.exists(filename_error):
                self.sender.send_files(filename_error, self.channel_id, filename_error,
                                       "Not Collected Supplies_{}".format(str(df.iloc[0, 1])))
                os.remove(filename_error)
        else:
            pass
        return '', 200
