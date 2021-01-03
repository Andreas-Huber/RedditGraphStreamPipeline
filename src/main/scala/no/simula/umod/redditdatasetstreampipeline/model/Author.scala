package no.simula.umod.redditdatasetstreampipeline.model

case class Author(
                    //author: Option[String],
                    created_utc: Int
                  ) extends ToCsv {

  override def toCsvSeq: Seq[String] = Seq(
    //author.getOrElse(""),
    created_utc.toString
  )

  override def getHeaders: Seq[String] = Seq(
    //"author",
    "created_utc"
  )
}

/*
Sample Author JSON:

{
   "author":"johnethen06_jasonbroken",
   "author_id":"2",
   "base10_id":2,
   "comment_karma":0,
   "created_utc":1397113483,
   "link_karma":0,
   "profile_over_18":null,
   "updated_utc":1592443109
}
*/
