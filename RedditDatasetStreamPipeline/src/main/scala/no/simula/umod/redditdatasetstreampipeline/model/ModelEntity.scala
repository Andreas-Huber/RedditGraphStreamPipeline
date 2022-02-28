package no.simula.umod.redditdatasetstreampipeline.model

object ModelEntity extends Enumeration {
  type ModelEntity = Value
  val SubmissionEntity, CommentEntity, AuthorEntity, UserInSubredditEntity = Value
}
