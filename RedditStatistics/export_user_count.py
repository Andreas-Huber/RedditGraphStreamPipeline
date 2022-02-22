"""
Export data from experiments csv structured like "subreddit,count".
"""
from settings_load import *
import pandas as pd
import os


def load_users_count_df(csv_name: str) -> pd.DataFrame:
    """
    Load user, count dataframe from csv
    :type csv_name: str
    """
    path = os.path.join(settings.EXPERIMENTS_DIR, csv_name)
    dataframe = pd.read_csv(path)
    dataframe.columns = ['subreddit', "count"]
    dataframe["count"] = dataframe["count"].astype("int32")
    return dataframe


def export_top_n_counts(dataset_name: str, n: int) -> None:
    print(f"Export {dataset_name}~top-{n}")

    df = load_users_count_df(f"{dataset_name}.csv")

    top_n = df.nlargest(n, 'count')
    # Not needed for now
    # top_n.to_csv(os.path.join(settings.FILTER_LISTS_DIR, f"{dataset_name}~top-{n}.csv"), index=False)
    top_n = top_n.drop(columns=['count'])
    top_n.to_csv(os.path.join(settings.FILTER_LISTS_DIR, f"{dataset_name}~top-{n}.txt"), index=False)


if __name__ == '__main__':

    for i in (5, 10, 100, 1000, 10000):
        export_top_n_counts("UsersInSubreddits_2005-2019", i)
        export_top_n_counts("UserContributionsInSubreddits_2005-2019", i)

        for year in range(2005, 2020):
            export_top_n_counts(f"UsersInSubreddits_{year}", i)
            export_top_n_counts(f"UserContributionsInSubreddits_{year}", i)
