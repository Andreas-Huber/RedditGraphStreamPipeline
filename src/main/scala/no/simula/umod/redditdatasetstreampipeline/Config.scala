package no.simula.umod.redditdatasetstreampipeline

import java.io.File

import no.simula.umod.redditdatasetstreampipeline.Experiment.Experiment
import no.simula.umod.redditdatasetstreampipeline.ProgramMode.ProgramMode


/**
 * Program configuration that is filled from the command line parameters.
 * @see Scopt Documentation [[https://github.com/scopt/scopt]]
 */
case class Config (
                    datasetDirectory: File = new File("redditdataset"),
                    numberOfConcurrentFiles: Int = Runtime.getRuntime.availableProcessors(),
                    programMode: ProgramMode = ProgramMode.None,
                    fileNameContainsFilter: String = "",
                    fileNameNotContainsFilter: String = "",

                    // Pass trough options
                    provideSubmissionsStream: Boolean = false,
                    provideCommentsStream: Boolean = false,
                    provideAuthorsStream: Boolean = false,
                    submissionsOutFile: File = new File("submissions.csv"),
                    commentsOutFile: File = new File("comments.csv"),
                    authorsOutFile: File = new File("authors.csv"),
                    enableCount: Boolean = false,
                    filterBySubreddits: File = null,

                    // Statistics options
                    experiment: Experiment = Experiment.None,
                    statisticsOutDir: File = new File(System.getProperty("user.home"))
)

object ProgramMode extends Enumeration {
  type ProgramMode = Value
  val None, PassTrough, Statistics = Value
}

object Experiment extends Enumeration {
  type Experiment = Value
  val None, UserContributionsInSubreddits, UsersInSubreddits = Value
}

