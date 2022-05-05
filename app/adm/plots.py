import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from tempfile import NamedTemporaryFile


def get_time_series_plot(df: pd.DataFrame, columns_map):
    fig = plt.figure(figsize=(20, 12), dpi=150)
    ax = fig.add_subplot()

    for y in columns_map["Y"]["columns"]:
        ax.plot(
            df[columns_map["X"]["column"]],
            df[y["column"]],
            label=y["title"]
        )

    if "points" in y.keys():
        ax.scatter(
            df.loc[df[y["points"]], columns_map["X"]["column"]],
            df.loc[df[y["points"]], y["column"]],
            marker='o',
            c='red'
        )
    plt.xlabel(columns_map["X"]["title"])
    plt.ylabel(columns_map["Y"]["title"])
    plt.title(columns_map["plot_title"])

    file = NamedTemporaryFile(delete=False, suffix='.png')
    plt.savefig(file.name)
    plt.clf()
    plt.close()
    return file


def barplot(df, columns_map):
    plt.figure(figsize=(20, 12), dpi=150)
    sns.barplot(x=df[columns_map.get('X').get('column')], y=df[columns_map.get('Y').get('column')])
    plt.xlabel(columns_map.get('X').get("title"))
    plt.ylabel(columns_map.get("Y").get("title"))
    plt.title(columns_map.get("plot_title"))
    file = NamedTemporaryFile(delete=False, suffix='.png')
    plt.savefig(file.name)
    plt.close()
    return file
