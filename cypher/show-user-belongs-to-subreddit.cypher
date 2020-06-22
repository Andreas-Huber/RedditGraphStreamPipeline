// Show user belongs to subreddit
MATCH (user:User)-[r1]->(subreddit:Subreddit)
WHERE subreddit.subreddit <> "reddit.com"
RETURN r1, user, subreddit
LIMIT 1000