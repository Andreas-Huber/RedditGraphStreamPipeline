package no.simula.umod.redditdatasetstreampipeline

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.ConsoleTools.{log, logDuration}
import no.simula.umod.redditdatasetstreampipeline.Experiment.Experiment
import no.simula.umod.redditdatasetstreampipeline.model.{CountPerSubreddit, CountPerSubredditFactory, UserInSubreddit, UserInSubredditFactory}

import java.nio.file.{Path, Paths}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Awaitable, Future}
import scala.sys.exit

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
   * Creates a source with all the paths to the files that are part of the dataset.
   * Takes the set filters into account.
   * @return
   */
  private def createCommentSubmissionSource() : Source[Path, NotUsed]  = {
    val datasetDirectory = config.datasetDirectory.getAbsolutePath
    println(f"InputDirectory:     ${datasetDirectory}")

    val submissionsDirectory = Paths.get(datasetDirectory, "submissions")
    val commentsDirectory = Paths.get(datasetDirectory, "comments")

    val filesSource: Source[Path, NotUsed] =
      Directory.ls(commentsDirectory).filter(p => filterFiles("RC_", p))
        .concat(Directory.ls(submissionsDirectory).filter(p => filterFiles("RS_", p)))

    filesSource
  }

  private def createStatisticsSink(experiment: Experiment): Sink[ByteString, Future[IOResult]] = {
    val outFile = Paths.get(config.statisticsOutDir.getAbsolutePath, s"$experiment.csv").toFile
    println(s"Writing to file:    $outFile")

    // Output
    val fileSink = FileIO.toPath(outFile.toPath)

    fileSink
  }


  /** Counts the experiment to count the contributions users made in subreddits. A contribution is a post or comment. */
  def runUserContributionsInSubreddits(experiment: Experiment) {

    // Output
    val fileSink = createStatisticsSink(experiment)

    // Input
    val filesSource = createCommentSubmissionSource()

    // Count the user contributions in a subreddit per file
    val countUserContributionInSubreddits: Flow[UserInSubreddit, CountPerSubreddit, NotUsed] =
      Flow[UserInSubreddit].concat(Source.single(UserInSubredditFactory.endOfStream())).statefulMapConcat { () =>
        val subreddits =  collection.mutable.Map[String, Int]()
        val startNanoTime = System.nanoTime()

        { element =>
          if (element.author.isDefined) {
            // Add or update count per subreddit
            subreddits.updateWith(element.subreddit.getOrElse("No-Subreddit-defined"))({
              case Some(count) => Some(count + 1)
              case None        => Some(1)
            })

            Nil
          } else if (element.isEndOfStream()) {
            // Return the counts per subreddit as a list.
            val subredditsImmutable = subreddits.map(kv => CountPerSubreddit(kv._1, kv._2)).toList

            logDuration(f"Count user in SR finished", startNanoTime)

            subredditsImmutable
          } else {
            // Do not emit anything
            Nil
          }
        }
      }

    // Merge the results of the counts per file into one dictionary
    val mergeResults = Flow[CountPerSubreddit].concat(Source.single(CountPerSubredditFactory.endOfStream())).statefulMapConcat { () =>
      val subreddits =  collection.mutable.Map[String, Int]()
      val startNanoTime = System.nanoTime()

      { element =>
        if (!element.subreddit.isEmpty) {
          // Add or sum up the count per subreddit
          subreddits.updateWith(element.subreddit)({
            case Some(count) => Some(count + element.count)
            case None        => Some(element.count)
          })

          Nil
        } else if (element.isEndOfStream()) {
          // Return the counts per subreddit as a list.
          val subredditsImmutable = subreddits.map(kv => CountPerSubreddit(kv._1, kv._2)).toList
          logDuration("Merge counts finished", startNanoTime)
          subredditsImmutable

        } else {
          // Do not emit anything
          Nil
        }
      }
    }

    val eventualResult = filesSource
      .flatMapMerge(numberOfThreads, file => {
        // Flow per file from here
        log(file)

        Flows.getCompressorInputStreamSource(file.toString)
          .via(Flows.ndJsonToUserInSubreddit)
          //          .filter(a => a.author.isDefined && a.author.get != "[deleted]")
          .via(countUserContributionInSubreddits).async
      }).async
      .via(mergeResults)
      .via(Flows.objectToCsv)
      .runWith(fileSink)

    waitForResult(eventualResult)
  }

  private def waitForResult[T](result: Awaitable[T]) = {
    println("Output is ready.")
    println("------------------------------------------")


    // Wait for the results
    try {
      Await.result(result, 365.days)
    }
    catch {
      case ex: Exception => {
        log(ex)
        exit(1)
      }
      case _: Throwable => {
        log(_)
        exit(1)
      }
    }
  }
}





