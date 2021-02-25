package no.simula.umod.redditdatasetstreampipeline.model

case class UserInSubreddit (
                    subreddit: Option[String],
                    author: Option[String]
                  ) extends SubredditEntity with StreamMarker {

  def isEndOfStream: Boolean = subreddit.isEmpty && author.isEmpty

  override def toCsvSeq: Seq[String] = Seq(
    subreddit.getOrElse(""),
    author.getOrElse(""),
  )

  override def getHeaders: Seq[String] = Seq(
    "subreddit",
    "author"
  )
}

object UserInSubredditFactory {
  def endOfStream() = UserInSubreddit(None, None)
}