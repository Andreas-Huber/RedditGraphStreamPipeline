package no.simula.umod.redditdatasetstreampipeline.model

case class UserInSubreddit (
                    subreddit: Option[String],
                    author: Option[String]
                  ) extends StreamMarker {

  def isEndOfStream: Boolean = subreddit.isEmpty && author.isEmpty
}

object UserInSubredditFactory {
  def endOfStream() = UserInSubreddit(None, None)
}