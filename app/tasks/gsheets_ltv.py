import pygsheets
import pandas as pd

from adm.redash import get_result
from config import API_KEY, SERVICE_FILE_GSHEET_LTV


def replace_diagonal(df: pd.DataFrame) -> pd.DataFrame:
    """The function replaces the values above the diagonal with zero"""
    cur = 2
    for row in range(0, df.shape[0]):
        for col in range(cur, df.shape[1]):
            if df.iloc[row, col]:
                continue
            else:
                df.iloc[row, col] = 0.0
        cur += 1
    return df


def update_cohort(id_query: int, datefrom: str, dateto: str, url: str, sheet_name: str, col_name: str,
                  start_col_data: str, start_col_users) -> None:
    """"""
    gsheet_client = pygsheets.authorize(service_file=SERVICE_FILE_GSHEET_LTV)
    wks = gsheet_client.open_by_url(url).worksheet_by_title(sheet_name)
    df = get_result(id_query, {"date": {"start": datefrom, "end": dateto}}, API_KEY)
    df_update = replace_diagonal(df.pivot_table(index=["cohort", "users"], columns="timeframe",
                                values=col_name, fill_value="").reset_index())
    wks.set_dataframe(df_update.loc[:, ~df_update.columns.isin(["cohort", "users"])], start_col_data, copy_head=False)
    wks.set_dataframe(df_update[["users"]], start_col_users, copy_head=False)
    return
