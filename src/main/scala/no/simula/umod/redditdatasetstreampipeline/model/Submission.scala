package no.simula.umod.redditdatasetstreampipeline.model

case class Submission(subreddit: Option[String], id: Option[String]) extends ToCsv {
  override def toCsvSeq: Seq[String] =  Seq(subreddit.getOrElse(""), id.getOrElse(""))

  override def getHeaders: Seq[String] = Seq("subreddit", "id")
}


