package no.simula.umod.redditdatasetstreampipeline

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{FileIO, Flow, Keep, Sink, Source}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.Experiment.Experiment
import no.simula.umod.redditdatasetstreampipeline.model.ModelEntity.{AuthorEntity, CommentEntity, ModelEntity, SubmissionEntity}
import no.simula.umod.redditdatasetstreampipeline.model.{Submission, SubmissionFactory}

import java.io.File
import java.nio.file.{Path, Paths}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

/**
 * Runs the Dataset Statistics mode.
 * @param actorSystem Akka actor system
 * @param config Command line config
 */
class Statistics(actorSystem: ActorSystem, config: Config) {

  private implicit val system: ActorSystem = actorSystem
  private val numberOfThreads = config.numberOfConcurrentFiles

  private def filterFiles(filePrefix: String, p: Path) = {
    val fileName = p.getFileName.toString

    fileName.startsWith(filePrefix) && fileName.contains(config.fileNameContainsFilter)
  }

  /**
   * Runs the pipeline for the given entity and directory (comments or submissions)
   *
   * @param subdirectory comments or submissions directory
   * @param filePrefix Prefix of the archive files (RS_ or RC_)
   * @param entityType Model to deserialize.
   * @param outFile Output file or named pipe.
   * @return
   */
  private def datasetPipe(subdirectory: String, filePrefix: String, entityType: ModelEntity, outFile: File): Future[IOResult] = {
    val inputDirectory = Paths.get(config.datasetDirectory.getAbsolutePath, subdirectory)

    println(f"Statistics entity: ${entityType.toString}")
    println(f"InputDirectory: $inputDirectory")

    val filesSource: Source[Path, NotUsed] = Directory.ls(inputDirectory).filter(p => filterFiles(filePrefix, p))

    val fileSink = FileIO.toPath(outFile.toPath)


    val bWordsLast = Flow[Submission].concat(Source.single(SubmissionFactory.endOfStream())).statefulMapConcat { () =>
      var stashedBWords: List[Submission] = Nil

      { element =>
        if (element.author.getOrElse("").startsWith("v")) {
          // prepend to stash and emit no element
          stashedBWords = element :: stashedBWords
          Nil
        } else if (element.isEndOfStream()) {
          // return in the stashed words in the order they got stashed
          stashedBWords.reverse
        } else {
          // Do not emit anything
          Nil
        }
      }
    }

    val eventualResult = filesSource
      .flatMapMerge(numberOfThreads, file => {
        println(file)

        Flows.getCompressorInputStreamSource(file.toString)
          .via(Flows.ndJsonToSubmission)
//          .map(a => {
//            println(a.author)
//            a
//          })
          .filter(a => a.author.isDefined && a.author.get != "[deleted]")
          .via(bWordsLast)
          .via(Flows.objectToCsv)
      })
      .runWith(fileSink)

    println(f"Out pipe is ready to be read: $outFile")

    eventualResult
  }

  /** Runs the pass trough with the given options and waits for it's completion. */
  def runUsersInSubreddits(experiment: Experiment) {

    val outFile = Paths.get(config.statisticsOutDir.getAbsolutePath, s"$experiment.txt").toFile
    println(s"Writing to file: $outFile")

    var result: Future[IOResult] = Future.never

    // Start the pipelines async
    //Todo: prefix
    result = datasetPipe("submissions", "RS_v2_2006-01", SubmissionEntity, outFile)


    // Wait for the results
    Await.result(result, 365.days)
  }
}
