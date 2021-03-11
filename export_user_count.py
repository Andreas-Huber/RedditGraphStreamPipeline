"""
Export data from experiments csv structured like "subreddit,count".
"""
from python_settings import settings
import settings as local_settings
import pandas as pd
import os

settings.configure(local_settings)
assert settings.configured


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
    df = load_users_count_df(f"{dataset_name}.csv")

    top_n = df.nlargest(n, 'count')
    top_n.to_csv(os.path.join(settings.PANDASOUT_DIR, f"{dataset_name}~top-{n}.csv"), index=False)
    top_n = top_n.drop(columns=['count'])
    top_n.to_csv(os.path.join(settings.PANDASOUT_DIR, f"{dataset_name}~top-{n}.txt"), index=False)


if __name__ == '__main__':

    for i in (5, 10, 100, 1000, 10000):
        export_top_n_counts("UsersInSubreddits_2005-2019", i)
        export_top_n_counts("UserContributionsInSubreddits_2005-2019", i)
