package model

import scala.collection.immutable

case class Submission(subreddit: String, id:String){
  def toSeq: immutable.Seq[String]  = Seq(subreddit, id)
}
