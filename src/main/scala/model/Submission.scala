package model

import scala.collection.immutable

case class Submission(subreddit: Option[String],  id: Option[String]){
  def toSeq: immutable.Seq[String]  = Seq(subreddit.getOrElse(""), id.getOrElse(""))
}
