import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("/home/andreas/experiments/UsersInSubreddits.csv")
df.columns = ['subreddit', "count"]

df = df[df["count"] > 1000000]

df.drop(columns=['subreddit'])
df["count"] = df["count"].astype("int32")


plt.hist(df['count'], color = 'blue', edgecolor = 'black',
         bins = int(180/2))
# plt.gca().set_xscale("log")

# Add labels
plt.title('Histogram of number of unique users per subreddit (over 1M users)')
plt.xlabel('Unique users')
plt.ylabel('Number of subreddits')

plt.tight_layout()
plt.savefig("UsersInSubreddits_hist_over_1M.png")

plt.show()



# import pandas as pd
# import matplotlib.pyplot as plt
#
# df = pd.read_csv("/home/andreas/experiments/UserContributionInSubreddits_2021-01-12.csv")
# df.columns = ['subreddit', "count"]
#
# # df = df[df["count"] > 1000000]
#
# df.drop(columns=['subreddit'])
# df["count"] = df["count"].astype("int32")
#
#
# plt.hist(df['count'], color = 'blue', edgecolor = 'black',
#          bins = int(180/2))
# # plt.gca().set_xscale("log")
#
# # Add labels
# plt.title('Histogram of number of contributions per subreddit (1M +)')
# plt.xlabel('Contributions')
# plt.ylabel('Number of subreddits')
#
# plt.tight_layout()
# plt.savefig("UserContributionsInSubreddits_hist.png")
#
# plt.show()


import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("/home/andreas/experiments/UserContributionInSubreddits_2021-01-12.csv")
# df = pd.read_csv("/home/andreas/experiments/UsersInSubreddits.csv")
df.columns = ['subreddit', "count"]
df["count"] = df["count"].astype("int32")

df.nlargest(10, 'count')


# Add labels
# plt.title('Histogram of number of unique users per subreddit (over 1M users)')
# plt.xlabel('Unique users')
# plt.ylabel('Number of subreddits')
#
# plt.tight_layout()
# plt.savefig("UsersInSubreddits_top20.png")
#
# plt.show()
