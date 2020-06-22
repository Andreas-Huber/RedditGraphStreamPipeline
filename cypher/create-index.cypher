// Create Index
CREATE CONSTRAINT ON (submission:Submission) ASSERT submission.id IS UNIQUE;

CREATE CONSTRAINT ON (subreddit:Subreddit) ASSERT subreddit.id IS UNIQUE;

CREATE CONSTRAINT ON (user:User) ASSERT user.id IS UNIQUE;