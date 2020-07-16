// Load submissions 
CALL apoc.periodic.iterate(
"CALL apoc.load.json('file:/RS_NEO') YIELD value AS row",
"	
	MERGE (submission:Submission {id:row.id})
    ON CREATE SET
        submission.id = row.id,
        submission.title = row.title



    MERGE (user:User {id:row.author})
    ON CREATE SET
        user.id = row.author,
        user.title = row.author



    MERGE (subreddit:Subreddit {id:row.subreddit_id})
    ON CREATE SET
        subreddit.id = row.subreddit_id,
        subreddit.subreddit = row.subreddit


    MERGE (user)-[:POSTED]->(submission)
    MERGE (user)-[:JOINED]->(subreddit)
    MERGE (post)-[:POSTEDIN]->(subreddit)
"
, {batchSize:10000})