# Reddit Graph Stream Pipeline

The Reddit Graph Stream Pipeline shows a stream-based approach to analyze, filter, and create graphs between subreddits. As a data source, it uses the Pushshift Reddit Dataset.

There are four steps required to build the subreddit graph because we have to limit the number of subreddits we consider to bring the graph down to a manageable size. 
1. Determine the top n subreddits
2. build filter list
3. build subset (use named pipe or intermediate file)
4. build graph (RedditGraphTool)




## Statistics mode

The statistics mode performs essential evaluations on the dataset. Currently, it counts either the users per subreddit or the number of contributions per subreddit.

Count the users in subreddits
(`-J-*` paremeters are used to pass parameters to the JVM, in this case memory allocations.)
``` bash
rdsp -J-Xmx850g -J-Xms850g statistics UsersInSubreddits --exclude 2020 --experiment-suffix 2005-2019 --dataset-dir ~/PushshiftReddit/ --statistics-out ~/experiments/
```

### Results
Precomputed results are provided in the [dataset]{https://datasets.simula.no/ExE1Dlex4PyE78q9BXkl/}. It contains CSV files for all subreddits plus filter lists for the top n subreddits to be used with the pass-through mode.

## Pass through mode
Reads the dataset and provides it as one single CSV or NDJSON stream.
The pass-through begins as soon as someone starts reading from the named pipe, e.g., the Graph Building tool.

``` bash
rdsp passtrough --comments --submissions --exclude 2020 --only-user-in-sr \
--filter-by-sr ~/filterlists/UsersInSubreddits_2005-2019~top-10000.txt \
--comment-out ~/NAMEDPIPE1 --submission-out ~/NAMEDPIPE2 \
--dataset-dir ~/PushshiftReddit/
```

## Graph Building
Build a subreddit graph from the data stream provided by the pass through mode.

``` bash
rgraph --mode CreateFromUserSubredditCsvAndExport ~/NAMEDPIPE1 ~/NAMEDPIPE2 \
--out-dot ~/graph.dot --out-edge-csv ~/graph.edge.csv --out-vertex-csv ~/graph.vertex.csv
```


## Prerequisites

### Dataset folder structure
The project requires a copy of the Pushshift Reddit Dataset with the same folder structure and filenames as [files.pushshift.io/reddit/](https://files.pushshift.io/reddit/).

```bash
redditdataset/submissions/RS_2021-06.zst
redditdataset/comments/RC_2021-06.zst
```

### System requirements
- Java 15
- 512 GB RAM: The statistics mode building for the full dataset uses up to 340 GB of allocated heap. The graph building requires up to 156 GB for 10.000 subreddits. For subsets of the dataset, one can get away with less. The pass-through mode is not limited by memory.
- Many CPU cores - strictly speaking, not a requirement, but processing on a notebook will take substantially longer.

> Processing time on a dual AMD EPYC 7601 (64 cores /128 threads in total) takes 3:40h for the statistics mode and 11:21h for the graph building.


## Build and run

Uses the Scala Build Tool (SBT) to build the project.
Use `sbt run` to run the project and `sbt test` to execute the tests.