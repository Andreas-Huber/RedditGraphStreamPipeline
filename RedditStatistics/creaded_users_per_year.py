import subprocess
import os
import tempfile
import pandas as pd
import matplotlib.pyplot as plt

def create_named_pipe(pipe: str) -> str:

    # Name with random suffix
    pipe_name = f"redditpipe_{pipe}_{next(tempfile._get_candidate_names())}"

    # Full path
    path = os.path.join(tempfile.gettempdir(), pipe_name)

    # Mode of the FIFO pipe to be created
    mode = 0o600

    # Create a FIFO named path
    os.mkfifo(path, mode)

    print(f"Named pipe created: {path}")

    return path


def start_rdsp(authors_path):

    rdsp_command = "rdsp " \
                   "passtrough " \
                   "--authors " \
                   "--dataset-dir /home/andreas/reddit/ " \
                   "--submission-out /home/andreas/submissions.csv " \
                   "--comment-out /home/andreas/comments.csv " \
                   f"--author-out {authors_path}"


    p = subprocess.Popen(rdsp_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p


def plot_authors(authors_file: str):
    df = pd.read_csv(authors_file)
    df.columns = ['created_utc']
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
    df["created_utc"] = df["created_utc"].astype("datetime64")
    df.groupby(df["created_utc"].dt.year).count().plot(kind="bar")

    # todo: Remove 2020
    # todo: filter jsonbroken authors?

    plt.savefig("created_users_per_year.png")
    plt.show()



if __name__ == '__main__':
    authors = create_named_pipe("authors")
    process = start_rdsp(authors)

    plot_authors(authors)

    # Read RDSP output and wait for completion
    for line in process.stdout.readlines():
        print(line)
    process.wait()
