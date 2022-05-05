from worker import Worker
import sql_functions
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from globals import date_dict
from typing import Tuple
from adm import redash
from adm.plots import get_time_series_plot
from config import API_KEY


class AnomaliesQuery():
    def __init__(self,
                 query_id: int,
                 column: str,
                 params: dict,
                 name: str,
                 start_date: str,
                 end_date: str,
                 period: int = 20,
                 n_stds: float = 2,
                 min_points: int = 20
                 ):
        self.query_id = query_id
        self.column = column,
        self.params = params,
        self.name = name,
        self.start_date = start_date
        self.end_date = end_date
        self.period = period
        self.n_stds = n_stds
        self.min_points = min_points


ANOMALIES_LIST = [
    AnomaliesQuery(query_id=56,
                   column="",
                   params={
                        "date": {
                            "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                            "end": datetime.today().strftime("%Y-%m-%d")
                        },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=10
                   ),

    AnomaliesQuery(query_id=56,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=56,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=1.8,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=56,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=1.7,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=56,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=1.7,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=56,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=10
                   ),

    AnomaliesQuery(query_id=56,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=1.5,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=56,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=56,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=1.8,
                   min_points=3
                   ),

    AnomaliesQuery(query_id=295,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=1.5,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=296,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=1.5,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=297,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=1.5,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=295,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=297,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=299,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=299,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=299,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=299,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=299,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=299,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=15
                   ),

    AnomaliesQuery(query_id=299,
                   column="",
                   params={
                       "date": {
                           "start": (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d"),
                           "end": datetime.today().strftime("%Y-%m-%d")
                       },
                   },
                   name="",
                   start_date=(datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d"),
                   end_date=datetime.today().strftime("%Y-%m-%d"),
                   period=7,
                   n_stds=2.0,
                   min_points=15
                   ),

]


def get_rolling_metrics(col: pd.Series, n: int = 20, n_stds: float = 2) -> Tuple[pd.Series, pd.Series, pd.Series]:
    col_std = col.rolling(n, min_periods=3).std()
    col_mean = col.rolling(n, min_periods=3).mean()
    upper_bound = col_mean + col_std * n_stds
    lower_bound = col_mean - col_std * n_stds
    return col_mean, upper_bound, lower_bound


def get_delta_first_last(x: pd.Series) -> float:
    col = list(x)
    if col:
        if col[0] == 0:
            return col[-1] / 1
        else:
            return (col[-1] - col[0]) / col[0]
    else:
        return 0


def format_delta(val: float, postfix: str = "") -> str:
    value_pct = round(val * 100, 1)
    prefix = "+" if value_pct > 0 else ""
    return f"{prefix}{value_pct}% {postfix}"


def draw_plot(name: str, title: str, df: pd.DataFrame):
    columns_map = {
        "X": {"column": "date", "title": "Дата"},
        "Y": {
            "columns": [
                {
                    "column": "data",
                    "title": "Заказы",
                    "points": "point"
                }
            ],
            "title": title,
        },
        "plot_title": f"{name}: {title}"
    }

    plot = get_time_series_plot(df, columns_map)
    return plot


def cheap_anomalies(data: pd.DataFrame, column: str, start_date: str, end_date: str, period: int, n_stds: float,
                    min_points: int):

    anomalies = []

    data = data.loc[:, ["date", column]]
    data = data.sort_values(by="date")

    if data[column].count() < min_points:
        return []

    data[column + "mean_col"], data[column + "upper_bound"], data[column + "lower_bound"] = get_rolling_metrics(data[column], period, n_stds)
    data["wow"] = data[column].rolling(7).apply(get_delta_first_last)
    data["wow_str"] = data["wow"].apply(lambda x: format_delta(x, "WoW"))

    data[column + "direction"] = np.where(
        data["wow"].apply(abs) < 0.1, 0,
        np.where(
            data[column] > data[column + "upper_bound"], 1,
            np.where(data[column] < data[column + "lower_bound"], -1, 0)
        )
    )

    data["date"] = pd.to_datetime(data["date"])

    data["is_anomaly"] = data.apply(lambda data: data[column + "direction"]
                                                 and data["date"] >= datetime.strptime(start_date, "%Y-%m-%d")  and
                                                 data["date"] <= datetime.strptime(end_date, "%Y-%m-%d"), axis=1).astype(bool)

    anomaly_days = (
        data[data["is_anomaly"]][["date", column + "direction", "wow_str"]]
            .assign(emoji=lambda data: np.where(data[column + "direction"] > 0, "⬆️", "⬇️"))
    )



    dates = anomaly_days.apply(lambda data: f"{data['emoji']}{data['date']}({data['wow_str']})", axis=1,
                               result_type="reduce")

    chart_df = pd.DataFrame({
        "date": data["date"],
        "data": data[column],
        "point": data["is_anomaly"]

    }).sort_values(by=["date"])

    if anomaly_days.shape[0]:
        anomalies = [
            *anomalies,
            {"slice": column, "dates": dates, "chart_df": chart_df}
        ]

    return sorted(anomalies, key=lambda x: x["slice"])


def get_text_for_anomalies(name: str, data: pd.DataFrame, query: int):
    text = f"*{name}:* \n" + f"`https://redash.com/queries/{query}` \n \n"
    if data.shape[0] != 0:
        for val in data.values:
            text += val + "\n" + "\n"
        return text
    else:
        pass


def get_text_for_empty(name: str, dates: [], query: int):
    text = f"*{name}:* \n" + f"`https://redash.com/queries/{query}` \n \n"
    return text + f"Аномалий за период {dates[0]} - {dates[1]} не нашел"


class Anomalies(Worker):
    def __init__(self, sender, channel_id):
        super().__init__(sender, channel_id)

    def send(self):
        for anomaly in ANOMALIES_LIST:
            anomalies = cheap_anomalies(data=redash.get_result(
                                id_query=anomaly.query_id,
                                params=anomaly.params[0],
                                api_key=API_KEY),
                                column=str(anomaly.column[0]),
                                start_date=anomaly.start_date, end_date=anomaly.end_date,
                                period=anomaly.period, n_stds=anomaly.n_stds, min_points=anomaly.min_points)
            if anomalies:
                text = get_text_for_anomalies(anomaly.name[0], anomalies[0]['dates'], query=anomaly.query_id)
                self.sender.send_message(self.channel_id, text)
                self.sender.send_files(draw_plot(anomaly.name[0], anomaly.name[0], anomalies[0]['chart_df']), self.channel_id, anomaly.name[0], anomaly.name[0])
            else:
                text = get_text_for_empty(anomaly.name[0], [anomaly.start_date, anomaly.end_date],
                                          query=anomaly.query_id)
                self.sender.send_message(self.channel_id, text)
