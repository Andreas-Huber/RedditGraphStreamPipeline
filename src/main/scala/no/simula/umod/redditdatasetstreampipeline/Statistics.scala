package no.simula.umod.redditdatasetstreampipeline

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{FileIO, Flow, Keep, Sink, Source}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.Experiment.Experiment
import no.simula.umod.redditdatasetstreampipeline.model.ModelEntity.{AuthorEntity, CommentEntity, ModelEntity, SubmissionEntity}
import no.simula.umod.redditdatasetstreampipeline.model.{CountPerSubreddit, CountPerSubredditFactory, Submission, SubmissionFactory, ToCsv, UserInSubreddit, UserInSubredditFactory}

import java.io.File
import java.nio.file.{Path, Paths}
import scala.::
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


  /** Counts the experiment to count the contributions users made in subreddits. A contribution is a post or comment. */
  def runUserContributionsInSubreddits(experiment: Experiment) {

    val outFile = Paths.get(config.statisticsOutDir.getAbsolutePath, s"$experiment.txt").toFile
    println(s"Writing to file: $outFile")

    // Input
    val datasetDirectory = config.datasetDirectory.getAbsolutePath
    println(f"InputDirectory: ${datasetDirectory.toString}")

    val submissionsDirectory = Paths.get(datasetDirectory, "submissions")
    val commentsDirectory = Paths.get(datasetDirectory, "comments")

    val filesSource: Source[Path, NotUsed] =
      Directory.ls(commentsDirectory).filter(p => filterFiles("RC_", p))
        .concat(Directory.ls(submissionsDirectory).filter(p => filterFiles("RS_", p)))

    // Output
    val fileSink = FileIO.toPath(outFile.toPath)


    // Count the user contributions in a subreddit per file
    val countUserContributionInSubreddits: Flow[UserInSubreddit, CountPerSubreddit, NotUsed] =
      Flow[UserInSubreddit].concat(Source.single(UserInSubredditFactory.endOfStream())).statefulMapConcat { () =>
        val subreddits =  collection.mutable.Map[String, Int]()

        { element =>
          if (element.author.isDefined) {
            // Add or update count per subreddit
            subreddits.updateWith(element.subreddit.get)({
              case Some(count) => Some(count + 1)
              case None        => Some(1)
            })

            Nil
          } else if (element.isEndOfStream()) {
            // Return the counts per subreddit as a list.
            val subredditsImmutable = subreddits.map(kv => CountPerSubreddit(kv._1, kv._2)).toList
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
        println(file)


        Flows.getCompressorInputStreamSource(file.toString)
          .via(Flows.ndJsonToUserInSubreddit)
          //          .filter(a => a.author.isDefined && a.author.get != "[deleted]")
          .via(countUserContributionInSubreddits).async
      }).async
      .via(mergeResults)
      .via(Flows.objectToCsv)
      .runWith(fileSink)

    println(f"Out pipe is ready to be read: $outFile")


    // Wait for the results
    Await.result(eventualResult, 365.days)
  }
}





