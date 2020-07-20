package model

import scala.collection.immutable

case class Submission(subreddit: String, id:String){
  def valueSeq: immutable.Seq[String]  = {
    Console.println(id + " " + subreddit)
    Seq(subreddit, id)
  }
}
