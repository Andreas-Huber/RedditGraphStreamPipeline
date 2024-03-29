package no.simula.umod.redditdatasetstreampipeline.model

import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

/**
 * Create the JSON formats and provide them implicitly
 */
object JsonFormats {
  implicit val submissionFormat: RootJsonFormat[Submission] = jsonFormat4(Submission)
  implicit val commentFormat: RootJsonFormat[Comment] = jsonFormat4(Comment)
  implicit val authorFormat: RootJsonFormat[Author] = jsonFormat3(Author)
  implicit val userInSubredditFormat: RootJsonFormat[UserInSubreddit] = jsonFormat2(UserInSubreddit)
}



