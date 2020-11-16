package no.simula.umod.redditdatasetstreampipeline

import java.io.File

import no.simula.umod.redditdatasetstreampipeline.Experiment.Experiment
import no.simula.umod.redditdatasetstreampipeline.ProgramMode.{ProgramMode}


/**
 * Program configuration that is filled from the command line parameters.
 * @see Scopt Documentation [[https://github.com/scopt/scopt]]
 */
case class Config (
  datasetDirectory: File = new File("redditdataset"),
  numberOfConcurrentFiles: Int = Runtime.getRuntime().availableProcessors() / 2 + 1,
  programMode: ProgramMode = ProgramMode.None,

  // Pass trough options
  provideSubmissionsStream: Boolean = false,
  provideCommentsStream: Boolean = false,
  submissionsOutFile: File = new File("submissions.csv"),
  commentsOutFile: File = new File("comments.csv"),

  // Statistics options
  experiment: Experiment = Experiment.None
)

object ProgramMode extends Enumeration {
  type ProgramMode = Value
  val None, PassTrough, Statistics = Value
}

object Experiment extends Enumeration {
  type Experiment = Value
  val None, UserCount = Value
}

