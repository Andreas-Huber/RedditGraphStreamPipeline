package no.simula.umod.redditdatasetstreampipeline.model

case class Comment(
                    subreddit: Option[String],
                    id: Option[String],
                    author: Option[String],
                    body: Option[String]
                  ) extends ToCsv {

  override def toCsvSeq: Seq[String] = Seq(
    subreddit.getOrElse(""),
    id.getOrElse(""),
    author.getOrElse(""),
    body.getOrElse("")
  )

  override def getHeaders: Seq[String] = Seq(
    "subreddit",
    "id",
    "author",
    "body"
  )
}

/*
Sample Comment JSON:

{
    "edited":false,
    "id":"7va2",
    "distinguished":null,
    "archived":true,
    "gilded":0,
    "created_utc":"1230768002",
    "ups":1,
    "author":"Envark",
    "score":1,
    "downs":0,
    "parent_id":"t3_7mq2x",
    "subreddit":"reddit.com",
    "author_flair_css_class":null,
    "controversiality":0,
    "retrieved_on":1428222113,
    "score_hidden":false,
    "author_flair_text":null,
    "link_id":"t3_7mq2x",
    "name":"t1_7va2",
    "subreddit_id":"t5_6",
    "body":"Awww...I was hoping this post was about the living planet."
}
*/
