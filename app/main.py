import requests
import logging
import adm.interactivity
import traceback

from datetime import datetime, timedelta
from flask import Flask, request
from flask_apscheduler import APScheduler
from general.general import General
from cheap_anomalies.anomalies import Anomalies
from config import *
from globals import *
from adm import admin
from sender import Sender
from supply.current_balance import CurrentBalance
from supply.get_invoices import GetInvoices
from supply.get_sales_items import GetSalesItems
from adm.jira import Jira
from analytics.update_purchase_correctioncoef import UpdatePurchaseCorrectionCoef
from analytics.update_supply_correctioncoef import UpdateSupplyCorrectionCoef
from tasks import gsheets_ltv
from adm import redash
from adm.verify import verify
from operation.operation_report import OperationReport
from supply.update_items_pv_core import UpdateItemsPvCore
from analytics.get_data_supply_hub_ds import GetDataSupplyHubDs
from supply.supply import Supply

app = Flask(__name__)

logging.basicConfig(filename='logs.log', level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")


class Config(object):
    SCHEDULER_API_ENABLED = True

app.config.from_object(Config())

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()


def update_date_dict(date_dict):
    delta = -admin.get_value_delta(id_region=2)
    date_dict['dateFrom'] = (datetime.strptime((datetime.today() + timedelta(days=-1)).strftime("%Y-%m-%d 00:00:00"),
                                               "%Y-%m-%d 00:00:00") + timedelta(hours=delta)).strftime("%Y-%m-%d %H:00:00")
    date_dict['dateTo'] = (datetime.strptime((datetime.today() + timedelta(days=0)).strftime("%Y-%m-%d 00:00:00"),
                                               "%Y-%m-%d 00:00:00") + timedelta(hours=delta)).strftime("%Y-%m-%d %H:00:00")
    date_dict['current_date'] = (datetime.strptime((datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00"),
                                               "%Y-%m-%d 00:00:00") + timedelta(hours=delta)).strftime("%Y-%m-%d %H:00:00")
    date_dict['date_now'] = (datetime.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
    date_dict['date_week_prev'] = (datetime.today() + timedelta(days=-8)).strftime("%Y-%m-%d")
    date_dict['dateToBQ'] = (datetime.today() + timedelta(days=-1)).strftime("%Y-%m-%d").replace('-', '')
    date_dict['dateFromBQ'] = (datetime.today() + timedelta(days=-7)).strftime("%Y-%m-%d").replace('-', '')
    date_dict['dateFromBQ2'] = (datetime.today() + timedelta(days=-21)).strftime("%Y-%m-%d").replace('-', '')
    date_dict['dow'] = (datetime.today() + timedelta(days=-1)).isoweekday()
    date_dict['lastSunday'] = (datetime.today() + timedelta(days=-datetime.today().isoweekday())).strftime("%Y-%m-%d")
    date_dict['start_date_supply_plot'] = (datetime.strptime((datetime.today() + timedelta(days=0)).strftime("%Y-%m-%d 00:00:00"),
                                               "%Y-%m-%d 00:00:00") + timedelta(hours=4)).strftime("%Y-%m-%d %H:00:00")
    date_dict['end_date_supply_plot'] = (datetime.strptime((datetime.today() + timedelta(days=0)).strftime("%Y-%m-%d 00:00:00"),
                                               "%Y-%m-%d 00:00:00") + timedelta(hours=20)).strftime("%Y-%m-%d %H:00:00")
    return


update_date_dict(date_dict)
main_sender = Sender(BOT_TOKEN)
general = General(main_sender, CHANNEL_NAME_GENERAL)
current_balance = CurrentBalance(main_sender, CHANNEL_NAME_BEST_SKU)
get_invoices = GetInvoices(main_sender, CHANNEL_NAME_BEST_SKU)
get_sales_items = GetSalesItems(main_sender, CHANNEL_NAME_BEST_SKU)
anomalies = Anomalies(main_sender, CHANNEL_NAME_ANOMALIES)
jira = Jira(main_sender, CHANNEL_NAME_DATA)
update_purchase_coeffs = UpdatePurchaseCorrectionCoef(main_sender, CHANNEL_NAME_DATA)
update_supply_coeffs = UpdateSupplyCorrectionCoef(main_sender, CHANNEL_NAME_DATA)
operation_reporting = OperationReport(main_sender, CHANNEL_NAME_OPERATION)
update_items_pv_core = UpdateItemsPvCore(main_sender, CHANNEL_NAME_TEST)
get_data_supply_hub_ds = GetDataSupplyHubDs(main_sender, CHANNEL_NAME_PV)
supply = Supply(main_sender, CHANNEL_NAME_PV)


@scheduler.task('cron', id='job_dict_update', hour='03', minute='30', misfire_grace_time=900)
def update_dict():
    update_date_dict(date_dict)
    app.logger.error('update_dict %s', date_dict)


@scheduler.task(trigger='interval', id='job_get_data_supply_hub_ds', minutes=20)
def send_get_data_supply_hub_ds():
    try:
        get_data_supply_hub_ds.send()
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@scheduler.task('cron', id='job_send_general', hour='06', minute='35', misfire_grace_time=900)
def send_general():
    try:
        general.send()
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@scheduler.task('cron', id='job_send_supply', hour='06', minute='45', misfire_grace_time=900)
def send_supply():
    try:
        supply.send()
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@scheduler.task('cron', id='job_send_jira', hour='07', minute='30', misfire_grace_time=900, day_of_week="mon-fri")
def send_jira():
    try:
        jira.send()
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@scheduler.task('cron', id='job_send_anomalies', hour='07', minute='30', misfire_grace_time=900, day_of_week='mon')
def send_anomalies():
    try:
        anomalies.send()
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@scheduler.task('cron', id='job_update_purchase_coeffs', hour='23', minute='00', misfire_grace_time=900)
def send_update_purchase_coeffs():
    try:
        update_purchase_coeffs.send()
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@scheduler.task('cron', id='job_update_supply_coeffs', hour='23', minute='15', misfire_grace_time=900)
def send_update_supply_coeffs():
    try:
        update_supply_coeffs.send()
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@scheduler.task('cron', id='job_update_gsheet', hour='07', minute='45', misfire_grace_time=900, day_of_week='mon')
def update_gsheet():
    try:
        gsheets_ltv.update_cohort(4, '2021-01-01', date_dict['lastSunday'], URL_GSHEET_LTV, WORKSHEET_GSHEET_LTV,
                                  "ltv", "I5", "C5")
        gsheets_ltv.update_cohort(152, '2021-01-01', date_dict['lastSunday'], URL_GSHEET_LTV, WORKSHEET_GSHEET_AOV,
                                  "aov", "C7", "B7")
        gsheets_ltv.update_cohort(163, '2021-01-01', date_dict['lastSunday'], URL_GSHEET_LTV, WORKSHEET_GSHEET_ORDERS,
                                  "orders", "C6", "B6")
        gsheets_ltv.update_cohort(164, '2021-01-01', date_dict['lastSunday'], URL_GSHEET_LTV, WORKSHEET_GSHEET_MARGIN,
                                  "margin", "C6", "B6")
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@scheduler.task('interval', id='refresh_dashboards', minutes=60)
def refresh_dashboards():
    try:
        redash.refresh_dashboard(REDASH_URL, API_KEY, DASHBOARD_SLUG_GENERAL)
        redash.refresh_dashboard(REDASH_URL, API_KEY, DASHBOARD_SLUG_MARKETING)
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@scheduler.task('cron', id='job_send_operation_reporting', hour='07', minute='00', misfire_grace_time=900)
def send_report_operation():
    try:
        operation_reporting.send()
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@app.route('/', methods=['GET'])
def healthcheck():
    return 'healthy', 200


@app.route('/testcommand', methods=['POST'])
def testcommand():
    if not verify(request):
        return '', 400
    return adm.interactivity.test_block, 200


@app.route('/sales', methods=['POST'])
def sales():
    try:
        return admin.get_job_with_timepicker(request, "sales", date_dict['date_week_prev'], date_dict['date_now'])
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@app.route('/balance', methods=['POST'])
def balance():
    try:
        return admin.get_job_without_params(request, current_balance.current_balance)
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@app.route('/invoices', methods=['POST'])
def invoices():
    try:
        return admin.get_job_with_timepicker(request, "invoices", date_dict['date_week_prev'], date_dict['date_now'])
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@app.route('/pvcore_update', methods=['POST'])
def pvcore_update():
    try:
        return update_items_pv_core.get_job_pv_core(request)
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())


@app.route('/', methods=['POST'])
def handle():
    try:
        if not verify(request):
            return '', 400
        params = admin.get_main_params_from_request(request)
        action_id = params["action_id"]

        if action_id == 'invoices_confirm':
            requests.post(params["response_url"],
                          json=adm.interactivity.acc_block)
            get_invoices.get_invoices(
                admin.get_dates_params_from_request(request)["datefrom"],
                admin.get_dates_params_from_request(request)["dateto"],
                params["user_id"])

        elif action_id == 'sales_confirm':
            requests.post(params["response_url"],
                          json=adm.interactivity.acc_block)
            get_sales_items.get_sales_items(
                admin.get_dates_params_from_request(request)["datefrom"],
                admin.get_dates_params_from_request(request)["dateto"],
                params["user_id"])

        elif action_id == "pvcore_update_confirm" or action_id == "pvcore_update_cancel":
            requests.post(params["response_url"],
                          json=adm.interactivity.acc_block)
            update_items_pv_core.update_items_pv_core(request, params["response_url"])

        return '', 200
    except Exception:
        Sender(BOT_TOKEN).send_message(CHANNEL_NAME_ERRORS, traceback.format_exc())
