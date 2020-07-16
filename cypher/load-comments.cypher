// Load comments 
CALL apoc.periodic.iterate(
"CALL apoc.load.json('file:/RC_NEO') YIELD value AS row",
"
	MERGE (comment:Comment {id:row.id})
    ON CREATE SET
        comment.id = row.id,
        comment.body = row.body,
        comment.submissionId = substring(row.link_id, 3)

	MERGE (submission:Submission {id:substring(row.link_id, 3)})
    ON CREATE SET
        submission.id = substring(row.link_id, 3)
        
	MERGE (user:User {id:row.author})
    ON CREATE SET
        user.id = row.author,
        user.title = row.author
        
    MERGE (subreddit:Subreddit {id:substring(row.subreddit_id, 3)})
    ON CREATE SET
        subreddit.id = substring(row.subreddit_id, 3),
        subreddit.subreddit = row.subreddit
	
    MERGE (user)-[:COMMENTED]->(comment)
    MERGE (user)-[:JOINED]->(subreddit)
    MERGE (comment)-[:COMMENTMADE]->(submission)
    
"
, {batchSize:10000})