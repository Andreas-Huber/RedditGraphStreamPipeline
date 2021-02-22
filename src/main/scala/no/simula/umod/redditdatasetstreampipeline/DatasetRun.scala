package no.simula.umod.redditdatasetstreampipeline

import akka.actor.ActorSystem

import java.nio.file.Path
import scala.collection.immutable
import scala.collection.immutable.HashSet
import scala.io.Source

abstract class DatasetRun(actorSystem: ActorSystem, config: Config) {
  protected implicit val system: ActorSystem = actorSystem
  protected val numberOfThreads = config.numberOfConcurrentFiles

  // Load filter list
  var subredditsToFilter: HashSet[String] = immutable.HashSet.empty[String]
  if(config.filterBySubreddits != null){
    subredditsToFilter = immutable.HashSet.from(Source.fromFile(config.filterBySubreddits).getLines().drop(1))
  }
  val filterBySubreddits: Boolean = subredditsToFilter != null && !subredditsToFilter.isEmpty

  /**
   * Filters the stream by subreddits, if the optional filter is configured via the config.
   * @param subreddit name of the subreddit
   * @return True if it shall be included in the output stream
   */
  protected def filterSubreddits(subreddit: Option[String]): Boolean = !filterBySubreddits || subredditsToFilter.contains(subreddit.getOrElse(""))

  /**
   * Filter the file name based on a prefix and the filters specified in the config
   * @param filePrefix the file name has to start with e.g. "RS_"
   * @param path to to the file that should be checked.
   * @return True if the file should be kept, false if the file should be ignored.
   */
  protected def filterFiles(filePrefix: String, path: Path): Boolean = {
    val fileName = path.getFileName.toString

    // Filter prefix
    fileName.startsWith(filePrefix) &&
    // Filter file name contains
      fileName.contains(config.fileNameContainsFilter) &&
    // Filter file name not contains - only if set
      (config.fileNameNotContainsFilter.isBlank || !fileName.contains(config.fileNameNotContainsFilter))
  }
}
