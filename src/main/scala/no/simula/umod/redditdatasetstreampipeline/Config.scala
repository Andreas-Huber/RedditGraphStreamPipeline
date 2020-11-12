package no.simula.umod.redditdatasetstreampipeline

import java.io.File

/**
 * Program configuration that is filled from the command line parameters.
 * @see Scopt Documentation [[https://github.com/scopt/scopt]]
 */
case class Config (
  datasetDirectory: File = new File("redditdataset"),
  provideSubmissionsStream: Boolean = false,
  provideCommentsStream: Boolean = false,
  submissionsOutFile: File = new File("submissions.csv"),
  commentsOutFile: File = new File("comments.csv"),
  numberOfConcurrentFiles: Int = Runtime.getRuntime().availableProcessors() / 2 + 1
)

