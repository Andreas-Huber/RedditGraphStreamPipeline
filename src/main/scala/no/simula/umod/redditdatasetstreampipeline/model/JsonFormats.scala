package no.simula.umod.redditdatasetstreampipeline.model

import spray.json.DefaultJsonProtocol._

/**
 * Create the JSON formats and provide them implicitly
 */
object JsonFormats {
  implicit val submissionFormat = jsonFormat2(Submission)

}
