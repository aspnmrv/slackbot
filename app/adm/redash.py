import pandas as pd
import time
import requests
import logging
import sender
import traceback
import json

from config import REDASH_URL, BOT_TOKEN
from redash_toolbelt import Redash, get_frontend_vals
from globals import CHANNEL_NAME_ERRORS

log = logging.getLogger('redash')


def get_query(id_query, api_key, params):
    """"""
    resp = requests.post(
        f"{REDASH_URL}api/queries/{id_query}/results",
        params={"api_key": api_key},
        json={"id": id_query, "parameters": params, "max_age": 0}
    ).json()

    if "job" in resp:
        return resp["job"]["id"]
    else:
        raise ValueError(str(resp))


def get_status_job(job_id, api_key):
    """"""
    resp = requests.get(
        f"{REDASH_URL}api/jobs/{job_id}",
        params={"api_key": api_key}
    ).json()
    if "job" in resp:
        if resp["job"]["status"] in (3, 4):
            return resp["job"]["query_result_id"]
        else:
            time.sleep(1)
            return get_status_job(job_id, api_key)
    elif "message" in resp:
        raise ValueError(resp["message"])
    else:
        ValueError(str(resp))


def get_data(result_id, api_key):
    """"""
    resp = requests.get(
        f"{REDASH_URL}api/query_results/{result_id}",
        params={"api_key": api_key}
    ).json()["query_result"]["data"]["rows"]

    return pd.DataFrame(resp)


def get_result(id_query, params, api_key):
    """"""
    try:
        id_job = get_query(id_query, api_key, params)
        id_result = get_status_job(id_job, api_key)
        result = get_data(id_result, api_key)
        return result
    except Exception:
        sender.Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


def refresh_dashboard(baseurl, apikey, slug):
    """"""
    try:
        client = Redash(baseurl, apikey)
        todays_dates = get_frontend_vals()
        queries_dict = get_queries_on_dashboard(client, slug)

        for idx, qry in queries_dict.items():

            params = {
                p.get("name"): fill_dynamic_val(todays_dates, p)
                for p in qry["options"].get("parameters", [])
            }

            body = {"parameters": params, "max_age": 0}

            r = client._post(f"api/queries/{idx}/results", json=body)

            print(f"Query: {idx} -- Code {r.status_code}")
    except Exception:
        sender.Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


def get_queries_on_dashboard(client, slug):
    """"""
    dash = client.dashboard(slug=slug)
    viz_widgets = [i for i in dash["widgets"] if "visualization" in i.keys()]
    l_query_ids = [i["visualization"]["query"]["id"] for i in viz_widgets]

    return {id: client._get(f"api/queries/{id}").json() for id in l_query_ids}


def fill_dynamic_val(dates, p):
    """"""
    if not is_dynamic_param(dates, p):
        return p.get("value")
    dyn_val = getattr(dates, p.get("value"))

    if is_date_range(dyn_val):
        return format_date_range(dyn_val)
    else:
        return format_date(dyn_val)


def is_dynamic_param(dates, param):
    """"""
    return "date" in param.get("type") and param.get("value") in dates._fields


def is_date_range(value):
    """"""
    return hasattr(value, "start") and hasattr(value, "end")


def format_date(date_obj):
    """"""
    return date_obj.strftime("%Y-%m-%d")


def format_date_range(date_range_obj):
    """"""
    start = format_date(date_range_obj.start)
    end = format_date(date_range_obj.end)

    return dict(start=start, end=end)
