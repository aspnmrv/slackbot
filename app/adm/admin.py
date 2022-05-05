import sql_functions
import pandas.io.sql as sqlio
import json
import os
import psycopg2
import pandas as pd
import logging
import sender
import threading
import adm.interactivity
import traceback
import requests

from datetime import datetime, timedelta
from psycopg2 import pool
from config import MINCONN, MAXCONN, BIGQUERY_PROJECT_ID, BOT_TOKEN
from globals import CHANNEL_NAME_ERRORS
from adm.verify import verify

log = logging.getLogger('admin')


def connect_from_config(file):
    with open(file, 'r') as fp:
        config = json.load(fp)
    return psycopg2.connect(**config)


def create_pool_from_config(minconn, maxconn, file):
    with open(file, 'r') as fp:
        config = json.load(fp)
    return pool.SimpleConnectionPool(minconn, maxconn, **config)


CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config.json')
CLIENT_BQ_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '')
GLOBAL_POOL = create_pool_from_config(MINCONN, MAXCONN, CONFIG_PATH)
project_id = BIGQUERY_PROJECT_ID


def reconnect():
    """"""
    global CONN_GLOBAL
    if CONN_GLOBAL.closed == 1:
        CONN_GLOBAL = connect_from_config(CONFIG_PATH)


def get_data_db(sql: str) -> pd.DataFrame:
    """"""
    try:
        conn = GLOBAL_POOL.getconn()
        data = sqlio.read_sql_query(sql, conn)
        GLOBAL_POOL.putconn(conn)
        if data.shape[0] > 0:
            return data
        else:
            return pd.DataFrame()
    except Exception:
        sender.Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


def update_data_db(sql: str) -> None:
    """"""
    try:
        conn = GLOBAL_POOL.getconn()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        GLOBAL_POOL.putconn(conn)
        return None
    except Exception:
        sender.Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


# def get_data_bq(sql: str) -> pd.DataFrame:
#     """"""
#     try:
#         return CLIENT_BQ.query(sql, project=project_id).to_dataframe()
#     except Exception:
#         sender.Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


def send_type_request(request, error_text=None):
    if error_text is None:
        sender.Sender(BOT_TOKEN).send_message(
            CHANNEL_NAME_ERRORS,
                "Запрос! ✳️ \n" + \
                str("Команда: " + request.form["command"]) + "\n" + \
                str("Username: " + request.form["user_name"])
        )
    else:
        sender.Sender(BOT_TOKEN).send_message(
            CHANNEL_NAME_ERRORS,
                "Ошибка при запросе! ❌❌❌ \n" + str(error_text) + \
                str("Метод: " + "get_dates_params_from_request") + "\n" + \
                str("Команда: " + request.form["command"]) + "\n" + \
                str("Username: " + request.form["user_name"])
        )
    return


def get_timedelta(date: str, id_region: int):
    """"""
    delta_hour = get_data_db(sql_functions.sql_time_delta(id_region=id_region)).iloc[0, 0]
    return datetime.strptime(date, "%Y-%m-%d") + timedelta(hours=-delta_hour)


def get_value_delta(id_region: int) -> int:
    """"""
    delta_hour = get_data_db(sql_functions.sql_time_delta(id_region=id_region)).iloc[0, 0]
    return int(delta_hour)


def get_timedelta_offset(date: str, delta: int):
    """"""
    return datetime.strptime(date, "%Y-%m-%d") + timedelta(hours=delta)


def get_job_without_params(request, target):
    """"""
    try:
        if not verify(request):
            return '', 400
        user_id = request.form.get('user_id')
        thr = threading.Thread(target=target, args=(user_id,))
        thr.start()
        send_type_request(request)
        return adm.interactivity.acc_block, 200
    except Exception:
        send_type_request(request, str(traceback.format_exc()))


def get_job_with_timepicker(request, target, datefrom, dateto):
    """"""
    try:
        if not verify(request):
            return '', 400
        send_type_request(request)
        return adm.interactivity.get_two_datepickers_block(datefrom, dateto, target), 200
    except Exception:
        send_type_request(request, str(traceback.format_exc()))


def get_interactivity(request, elem):
    """"""
    try:
        if not verify(request):
            return '', 400
        send_type_request(request)
        return elem, 200
    except Exception:
        send_type_request(request, str(traceback.format_exc()))


def get_text_interactivity(request, elem):
    """"""
    try:
        if not verify(request):
            return '', 400
        return elem, 200
    except Exception:
        send_type_request(request, str(traceback.format_exc()))


def send_message_with_url(response_url, text):
    requests.post(response_url, json={
        "text": text
    })


def get_main_params_from_request(request) -> dict:
    """"""
    try:
        user_id = json.loads(request.form.get('payload')).get('user').get('id')
        action_id = json.loads(request.form.get('payload')).get('actions')[0] \
            .get('action_id')
        response_url = json.loads(request.form.get('payload')) \
            .get('response_url')
        action_ts = json.loads(json.loads(request.form.get('payload')).get('actions')[0] \
            .get('action_ts'))
        return {
            "user_id": user_id,
            "action_id": action_id,
            "response_url": response_url,
            "action_ts": action_ts
        }
    except Exception:
        send_type_request(request, str(traceback.format_exc()))


def get_text_from_request(request) -> str:
    """"""
    try:
        return request.form.get('text')
    except Exception:
        send_type_request(request, str(traceback.format_exc()))


def get_user_from_request(request) -> str:
    """"""
    try:
        return request.form.get('user_id')
    except Exception:
        send_type_request(request, str(traceback.format_exc()))


def get_dates_params_from_request(request) -> dict:
    """"""
    try:
        key = json.loads(request.form.get('payload')).get('actions')[0] \
            .get('value')
        block_id = json.loads(request.form.get('payload')).get('actions')[0] \
            .get('block_id')

        datefrom = json.loads(request.form.get('payload')) \
            .get('state') \
            .get('values') \
            .get(block_id) \
            .get(f'{key}_start') \
            .get('selected_date')

        dateto = json.loads(request.form.get('payload')) \
            .get('state') \
            .get('values') \
            .get(block_id) \
            .get(f'{key}_end') \
            .get('selected_date')

        return {
            "datefrom": datefrom,
            "dateto": dateto
        }
    except Exception:
        send_type_request(request, str(traceback.format_exc()))
