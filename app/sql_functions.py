import pandas.io.sql as sqlio
import json
import os
import psycopg2

from psycopg2 import pool
from config import MINCONN, MAXCONN
from typing import List


def connect_from_config(file):
    with open(file, 'r') as fp:
        config = json.load(fp)
    return psycopg2.connect(**config)


def create_pool_from_config(minconn, maxconn, file):
    with open(file, 'r') as fp:
        config = json.load(fp)
    return pool.SimpleConnectionPool(minconn, maxconn, **config)


CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')

GLOBAL_POOL = create_pool_from_config(MINCONN, MAXCONN, CONFIG_PATH)


def reconnect():
    global CONN_GLOBAL
    if CONN_GLOBAL.closed == 1:
        CONN_GLOBAL = connect_from_config(CONFIG_PATH)


def sql_count_requests(dateFrom, dateTo):
    conn = GLOBAL_POOL.getconn()
    sql = f'''
        '''
    count_requests = sqlio.read_sql_query(sql, conn)
    count_requests = count_requests.iloc[0, 0]
    GLOBAL_POOL.putconn(conn)
    return count_requests


def sql_count_requests_test(datefrom, dateto) -> str:
    sql = f'''
        '''
    return sql


def sql_retention_month(date) -> str:
    sql = f'''
    '''
    return sql


def sql_aov_month(date) -> str:
    sql = f'''
    '''
    return sql


def sql_aar_month(date) -> str:
    sql = f'''
    '''
    return sql


def sql_gmv(datefrom, dateto) -> str:
    sql = f'''
    '''
    return sql


def sql_aov(datefrom, dateto) -> str:
    sql = f'''
    '''
    return sql


def sql_count_total_requests(dateto):
    conn = GLOBAL_POOL.getconn()
    sql = f'''
    ;
    '''
    count_total = sqlio.read_sql_query(sql, conn)
    count_total = count_total.iloc[0, 0]
    GLOBAL_POOL.putconn(conn)
    return count_total


def sql_count_total_requests_test(dateto) -> str:
    sql = f'''
    '''
    return sql


def sql_pv_test(datefrom, dateto) -> str:
    sql = f'''
        '''
    return sql


def sql_pv(datefrom, dateto):
    conn = GLOBAL_POOL.getconn()
    sql = f'''
        '''
    pv = sqlio.read_sql_query(sql, conn)
    pv = pv.iloc[0, 0]
    GLOBAL_POOL.putconn(conn)
    return pv


def sql_pv_ab(datefrom, dateto):
    conn = GLOBAL_POOL.getconn()
    sql = f'''
        '''
    pv_ab = sqlio.read_sql_query(sql, conn)
    pv_ab = pv_ab.iloc[0, 0]
    GLOBAL_POOL.putconn(conn)
    return pv_ab


def sql_pv_ab_test(datefrom, dateto) -> str:
    sql = f'''
        '''
    return sql


def sql_gmv_test(datefrom, dateto) -> str:
    sql = f'''
        '''
    return sql


def sql_psb(datefrom, dateto) -> str:
    sql = f'''
        '''
    return sql


def sql_pv_core(datefrom, dateto) -> str:
    sql = f'''
        '''
    return sql


def sql_new_users(datefrom, dateto):
    conn = GLOBAL_POOL.getconn()
    sql = f'''
        '''
    users = sqlio.read_sql_query(sql, conn)
    users = users.iloc[0, 0]
    GLOBAL_POOL.putconn(conn)
    return users


def sql_new_users_test(datefrom, dateto) -> str:
    sql = f'''
        '''
    return sql


def sql_time_delta(id_region: int) -> str:
    sql = f"""
    """
    return sql


def sql_acr_rpr_orders(dateFrom, dateTo, id_region):
    conn = GLOBAL_POOL.getconn()
    sql = f"""
    """
    df_acr_rpr = sqlio.read_sql_query(sql, conn)
    df_acr_rpr.to_excel(f'info{dateFrom}-{dateTo}.xlsx')
    GLOBAL_POOL.putconn(conn)
    return



def sql_invoices(dateFrom, dateTo):
    conn = GLOBAL_POOL.getconn()
    sql = f""
    try:
        df_invoices = sqlio.read_sql_query(sql, conn)
        df_invoices.to_excel(f"invoices{dateFrom}-{dateTo}.xlsx")
    finally:
        GLOBAL_POOL.putconn(conn)
    return


def sql_invoices_test(datefrom, dateto) -> str:
    sql = f""
    return sql


def sql_sales_items(datefrom, dateto, id_region):
    conn = GLOBAL_POOL.getconn()
    sql = f"""
    """
    try:
        df_sales_items = sqlio.read_sql_query(sql, conn)
        df_sales_items.to_excel(f'sales_items{datefrom}-{dateto}.xlsx')
    finally:
        GLOBAL_POOL.putconn(conn)
    return


def sql_sales_items_test(datefrom, dateto, id_region) -> str:
    sql = f"""
    """
    return sql


def sql_get_name_of_items(items: List[str]) -> str:
    sql = '''
        select
            id,
            name
        from table
        where table.id in ({items})
        '''.format(items=",".join(items))
    return sql


def sql_update_pv_core_items(items: List[str]) -> str:
    sql = '''
    '''.format(items=items)
    return sql


def sql_remove_items_from_pv_core() -> str:
    sql = '''
    '''
    return sql


def sql_get_pv_core_items() -> str:
    sql = '''
    '''
    return sql


def sql_get_new_supplies(datefrom, dateto) -> str:
    """"""
    sql = f'''
    '''
    return sql


def sql_get_data_for_plot(datefrom, dateto) -> str:
    """"""
    sql = f'''
    '''
    return sql


def sql_get_items() -> str:
    sql = '''

    '''
    return sql


def sql_get_data_for_plot_send(datefrom, dateto) -> str:
    """"""
    sql = f'''
    '''
    return sql
