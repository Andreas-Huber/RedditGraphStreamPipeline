# import subprocess
# import os
#
#
#
# def read_pipeline(directory):
#
#     rdsp_command = "rdsp " \
#                    "passtrough " \
#                    "--authors " \
#                    "--dataset-dir /home/andreas/redditloc/ " \
#                    "--submissions-out /home/andreas/submissions.csv " \
#                    "--comments-out /home/andreas/comments.csv " \
#                    "--authors-out /home/andreas/authors.csv "
#
#     p = subprocess.Popen(rdsp_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#     for line in p.stdout.readlines():
#         print(line)
#     p.wait()



# if __name__ == '__main__':
#     dataset_directory = "/home/andreas/gdreddit/"
#
#     read_pd()


import pandas as pd
import matplotlib.pyplot as plt
df = pd.read_csv("/home/andreas/authors.csv")
df.columns = ['created_utc']
df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
df["created_utc"] = df["created_utc"].astype("datetime64")
df.groupby(df["created_utc"].dt.year).count().plot(kind="bar")

# todo: Remove 2020
# todo: filter jsonbroken authors?

plt.savefig("create_users_per_year.png")
plt.show()
