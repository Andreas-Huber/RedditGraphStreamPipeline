// Load comments parent child relations
CALL apoc.periodic.iterate(
"CALL apoc.load.json('file:/RC_NEO') YIELD value AS row WITH * WHERE NOT row.author = '[deleted]' AND NOT row.body = '[deleted]' AND substring(row.parent_id, 0,2) = 't1' RETURN *",
"
	    
	MERGE (comment:Comment {id:row.id})
    ON CREATE SET
        comment.id = row.id

	MERGE (parentComment:Comment {id:substring(row.parent_id, 3)})
    ON CREATE SET
        comment.id = substring(row.parent_id,3)
        
    MERGE (comment)-[:ISREPLYTO]->(parentComment)
"
, {batchSize:10000})