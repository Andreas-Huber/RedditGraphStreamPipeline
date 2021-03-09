package no.simula.umod.redditdatasetstreampipeline

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.ConsoleTools.{log, logDuration}
import no.simula.umod.redditdatasetstreampipeline.model.{CountPerSubreddit, CountPerSubredditFactory, UserInSubreddit, UserInSubredditFactory}

import java.nio.file.{Path, Paths}
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Awaitable, Future}
import scala.sys.exit

/**
 * Runs the Dataset Statistics mode.
 *
 * @param actorSystem Akka actor system
 * @param config      Command line config
 */
class Statistics(actorSystem: ActorSystem, config: Config) extends DatasetRun(actorSystem, config) {

  /**
   * Flow to merge the results of the counts per subreddits into one map
   */
  private val mergeResults = Flow[CountPerSubreddit].concat(
    Source.single(CountPerSubredditFactory.endOfStream())
  ).statefulMapConcat { () =>

    val subreddits = collection.mutable.HashMap[String, Int]()
    val startNanoTime = System.nanoTime()

    { element =>
      if (!element.subreddit.isEmpty) {
        // Add or sum up the count per subreddit
        subreddits.updateWith(element.subreddit)({
          case Some(count) => Some(count + element.count)
          case None => Some(element.count)
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

  /** Experiment to count the number of contributions users made in subreddits.
   * A contribution is a post or comment. */
  def runUserContributionsInSubreddits() {

    // Source / sink
    val fileSink = createStatisticsSink()
    val filesSource = createCommentSubmissionSource()

    // Count the user contributions in a subreddit per file
    val countUserContributionInSubredditsPerFile: Flow[UserInSubreddit, CountPerSubreddit, NotUsed] =
      Flow[UserInSubreddit].concat(Source.single(UserInSubredditFactory.endOfStream())).statefulMapConcat { () =>
        val subreddits = collection.mutable.HashMap[String, Int]()
        val startNanoTime = System.nanoTime()

        { element =>
          if (element.author.isDefined) {
            // Add or update count per subreddit
            subreddits.updateWith(element.subreddit.getOrElse("No-Subreddit-defined"))({
              case Some(count) => Some(count + 1)
              case None => Some(1)
            })

            Nil
          } else if (element.isEndOfStream) {
            // Return the counts per subreddit as a list.
            val subredditsImmutable = subreddits.map(kv => CountPerSubreddit(kv._1, kv._2)).toList

            logDuration(f"Count user contribution in SR finished", startNanoTime)

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
          .via(countUserContributionInSubredditsPerFile).async
      }).async
      .via(mergeResults)
      .via(Flows.objectToCsv)
      .runWith(fileSink)

    waitForResult(eventualResult)
  }



  // Count the user, that made a contribution in a subreddit per file
  val countUsersInSubredditsPerFile: Flow[UserInSubreddit, CountPerSubreddit, NotUsed] =
    Flow[UserInSubreddit].concat(Source.single(UserInSubredditFactory.endOfStream())).statefulMapConcat { () =>

      val subreddits: mutable.HashMap[String, mutable.HashMap[String, Int]] = collection.mutable.HashMap()
      val startNanoTime = System.nanoTime()

      // Remarks: Increasing the count per author is not necessary. But it should not increase the duration
      //          and memory consumption to much. Might be useful for late.

      { element =>
        if (element.author.isDefined) {
          // Add or update count per subreddit
          subreddits.updateWith(element.subreddit.getOrElse("No-Subreddit-defined"))({
            // Update the map for the subreddit
            case Some(old) =>
              old.updateWith(element.author.get)({
                // Author exists, increase count
                case Some(count) => Some(count + 1)
                // First entry for this author
                case None => Some(1)
              })
              Some(old)

            // Create new entry for subreddit with current author and a count of 1
            case None => Some(mutable.HashMap(element.author.get -> 1))
          })

          Nil
        } else if (element.isEndOfStream) {
          // Return the counts per subreddit as a list.


          val subredditsImmutable = subreddits.map(kv => CountPerSubreddit(kv._1, kv._2.size)).toList

          logDuration(f"Count user in SR finished", startNanoTime)

          subredditsImmutable
        } else {
          // Do not emit anything
          Nil
        }
      }
    }

  /** Experiment to count the users that made at least one contribution in a subreddit.
   * A contribution is a post or comment. */
  def runUsersInSubreddits() {

    // Source / sink
    val fileSink = createStatisticsSink()
    val filesSource = createCommentSubmissionSource()




    val eventualResult = filesSource
      .flatMapMerge(numberOfThreads, file => {
        // Flow per file from here
        log(file)

        Flows.getCompressorInputStreamSource(file.toString)
          .via(Flows.ndJsonToUserInSubreddit)
          // Ignore deleted users completely, since n>1 deleted users would always be counted as 1.
          // Therefore Subreddits with only deleted users wont be in the result
          .filter(a => a.author.isDefined && a.author.get != "[deleted]")
          .via(countUsersInSubredditsPerFile).async
      }).async
      .via(mergeResults)
      .via(Flows.objectToCsv)
      .runWith(fileSink)

    waitForResult(eventualResult)
  }


  /**
   * Creates a source with all the paths to the files that are part of the dataset.
   * Takes the set filters into account.
   *
   * @return
   */
  private def createCommentSubmissionSource(): Source[Path, NotUsed] = {
    val datasetDirectory = config.datasetDirectory.getAbsolutePath
    println(f"InputDirectory:     ${datasetDirectory}")

    val submissionsDirectory = Paths.get(datasetDirectory, "submissions")
    val commentsDirectory = Paths.get(datasetDirectory, "comments")

    val filesSource: Source[Path, NotUsed] =
      Directory.ls(commentsDirectory).filter(p => filterFiles("RC_", p))
        .concat(Directory.ls(submissionsDirectory).filter(p => filterFiles("RS_", p)))

    filesSource
  }

  /** Create a file sink based on the experiment */
  private def createStatisticsSink(): Sink[ByteString, Future[IOResult]] = {
    val suffix = if(!config.experimentSuffix.isBlank) s"_${config.experimentSuffix}" else ""
    val fileName = s"${config.experiment}$suffix.csv"
    val outFile = Paths.get(config.statisticsOutDir.getAbsolutePath, fileName).toFile
    println(s"Writing to file:    $outFile")

    getFileSink(outFile)
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
      case tr: Throwable => {
        log(tr)
        exit(1)
      }
    }
  }
}





