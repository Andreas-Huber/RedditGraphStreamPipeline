package no.simula.umod.redditdatasetstreampipeline

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.ConsoleTools.log
import no.simula.umod.redditdatasetstreampipeline.model.ModelEntity.{AuthorEntity, CommentEntity, ModelEntity, SubmissionEntity}

import java.io.File
import java.nio.file.{Path, Paths}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

/**
 * Runs the Dataset PassTrough mode.
 * Pass trough mode provides the compressed dataset as a CSV stream.
 * One stream from submissions, one for comments.
 * @param actorSystem Akka actor system
 * @param config Command line config
 */
class PassTrough(actorSystem: ActorSystem, config: Config) extends DatasetRun(actorSystem, config) {

  // Divide "threads" between two streams
  if(config.provideSubmissionsStream && config.provideCommentsStream){
    Integer.max(1, numberOfThreads / 2)
  }

  /**
   * Runs the pipeline for the given entity and directory (comments or submissions)
   * and counts the results
   * @param subdirectory comments or submissions directory
   * @param filePrefix Prefix of the archive files (RS_ or RC_)
   * @param entityType Model to deserialize.
   * @param outFile Output file or named pipe.
   * @return
   */
  private def datasetPipeAndCount(subdirectory: String, filePrefix: String, entityType: ModelEntity, outFile: File): (Future[IOResult], Future[Int]) = {
    val inputDirectory = Paths.get(config.datasetDirectory.getAbsolutePath, subdirectory)

    println(f"PassTrough entity:  ${entityType.toString}")
    println(f"InputDirectory:     $inputDirectory")


    val filesSource: Source[Path, NotUsed] = Directory.ls(inputDirectory).filter(p => filterFiles(filePrefix, p))

    val fileSink = getFileSink(outFile)
    val countSink = Sink.fold[Int, ByteString](0)((acc, _) => acc + 1)

    val (eventualResult, countResult) = filesSource
      .flatMapMerge(numberOfThreads, file => {
        println(file)

        if (config.keepOriginalJson) {
          // Only filter original json lines
          Flows.getCompressorInputStreamSource(file.toString)
            .via(Flows.splitLines)
            .filter(l => filterJsonBySubreddit(l))
            .map(bs => bs ++ newLineByteString)
        } else {
          // Convert to csv
          Flows.getCompressorInputStreamSource(file.toString)
            .via(Flows.ndJsonToObj(entityType))
            .filter(e => filterSubreddits(e.subreddit))
            .via(Flows.objectToCsv)
        }
      })
      .alsoToMat(fileSink)(Keep.right)
      .toMat(countSink)(Keep.both)
      .run()


    println(f"Out pipe is ready to be read: $outFile")

    (eventualResult, countResult)
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

    println(f"PassTrough entity: ${entityType.toString}")
    println(f"InputDirectory: $inputDirectory")

    val fileSink = getFileSink(outFile)
    val filesSource: Source[Path, NotUsed] = Directory.ls(inputDirectory).filter(p => filterFiles(filePrefix, p))

    val eventualResult: Future[IOResult] = filesSource
      .flatMapMerge(numberOfThreads, file => {
        log(file)

        if (config.keepOriginalJson) {
          // Only filter original json lines
          Flows.getCompressorInputStreamSource(file.toString)
            .via(Flows.splitLines)
            .filter(l => filterJsonBySubreddit(l))
            .map(bs => bs ++ newLineByteString)
        } else {
          // Convert json to CSV
          Flows.getCompressorInputStreamSource(file.toString)
            .via(Flows.ndJsonToObj(entityType))
            .filter(e => filterSubreddits(e.subreddit))
            .via(Flows.objectToCsv)
        }
      })
      .runWith(fileSink)

    println(f"Out pipe is ready to be read: $outFile")

    eventualResult
  }


  /** Runs the pass trough with the given options and waits for it's completion. */
  def runPassTrough() {

    var submissionResult: Future[IOResult] = Future.never
    var submissionCountResult: Future[Int] = Future.never

    var commentsResult: Future[IOResult] = Future.never
    var commentsCountResult: Future[Int] = Future.never

    var authorsResult: Future[IOResult] = Future.never
    var authorsCountResult: Future[Int] = Future.never


    // Start the pipelines async
    if(config.provideCommentsStream){
      if (config.enableCount){
        val (result, countResult)  = datasetPipeAndCount("comments", "RC_", CommentEntity, config.commentsOutFile)
        commentsResult = result
        commentsCountResult = countResult
      } else {
        commentsResult = datasetPipe("comments", "RC_", CommentEntity, config.commentsOutFile)
      }
    }

    if(config.provideSubmissionsStream){
      if(config.enableCount){
        val (result, countResult)  = datasetPipeAndCount("submissions", "RS_", SubmissionEntity, config.submissionsOutFile)
        submissionResult = result
        submissionCountResult = countResult
      } else {
        submissionResult = datasetPipe("submissions", "RS_", SubmissionEntity, config.submissionsOutFile)
      }
    }

    if(config.provideAuthorsStream){
      if (config.enableCount){
        val (result, countResult)  = datasetPipeAndCount("authors", "RA_2020-06-28.ndjson.zst", AuthorEntity, config.authorsOutFile)
        authorsResult = result
        authorsCountResult = countResult
      } else {
        authorsResult = datasetPipe("authors", "RA_2020-06-28.ndjson.zst", AuthorEntity, config.authorsOutFile)
      }
    }


    // Wait for the results
    if(config.provideCommentsStream){
      if(config.enableCount){
        val count = Await.result(commentsCountResult, 365.days)
        println(f"Count Comments: $count")
      }
      Await.result(commentsResult, 365.days)
    }

    if(config.provideSubmissionsStream){
      if(config.enableCount){
      val count = Await.result(submissionCountResult, 365.days)
      println(f"Count Submissions: $count")
      }
      Await.result(submissionResult, 365.days)
    }

    if(config.provideAuthorsStream){
      if(config.enableCount){
        val count = Await.result(authorsCountResult, 365.days)
        println(f"Count Authors: $count")
      }
      Await.result(authorsResult, 365.days)
    }
  }
}
