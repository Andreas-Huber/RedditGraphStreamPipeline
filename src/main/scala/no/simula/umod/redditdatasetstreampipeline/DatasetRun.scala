package no.simula.umod.redditdatasetstreampipeline

import akka.actor.ActorSystem

import java.nio.file.Path

abstract class DatasetRun(actorSystem: ActorSystem, config: Config) {
  protected implicit val system: ActorSystem = actorSystem
  protected val numberOfThreads = config.numberOfConcurrentFiles

  /**
   * Filter the file name based on a prefix and the filters specified in the config
   * @param filePrefix the file name has to start with e.g. "RS_"
   * @param path to to the file that should be checked.
   * @return True if the file should be kept, false if the file should be ignored.
   */
  protected def filterFiles(filePrefix: String, path: Path) = {
    val fileName = path.getFileName.toString

    // Filter prefix
    fileName.startsWith(filePrefix) &&
    // Filter file name contains
      fileName.contains(config.fileNameContainsFilter) &&
    // Filter file name not contains - only if set
      (config.fileNameNotContainsFilter.isBlank || !fileName.contains(config.fileNameNotContainsFilter))
  }
}
