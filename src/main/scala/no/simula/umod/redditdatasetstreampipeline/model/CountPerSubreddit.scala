package no.simula.umod.redditdatasetstreampipeline.model

case class CountPerSubreddit(
                              subreddit: String,
                              count: Int
                            ) extends ToCsv {

  override def toCsvSeq: Seq[String] = Seq(
    subreddit,
    count.toString
  )

  override def getHeaders: Seq[String] = Seq(
    "subreddit",
    "count",
  )

  def isEndOfStream(): Boolean = subreddit.isEmpty && count == 0
}

object CountPerSubredditFactory {

  def endOfStream() = CountPerSubreddit("", 0)
}