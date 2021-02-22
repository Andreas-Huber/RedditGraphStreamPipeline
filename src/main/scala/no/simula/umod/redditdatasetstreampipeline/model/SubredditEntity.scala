package no.simula.umod.redditdatasetstreampipeline.model

trait SubredditEntity extends ToCsv {
  def subreddit: Option[String]
  def author: Option[String]
}
