package no.simula.umod.redditdatasetstreampipeline

import java.io.{BufferedInputStream, File, FileInputStream, FileNotFoundException}
import java.nio.file.{Path, Paths}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source, StreamConverters}
import akka.util.ByteString
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt

object Main extends App {
  implicit val system = ActorSystem("ReadArchives")
  val fileIn = "C:\\import\\RS_v2_2008-03.gz"
  val fileOut = "C:\\_\\_RS.out"
  val submissionsDirectory = Paths.get("C:\\import\\submissions\\");
  val numberOfThreads = 6;


  val startTime = System.nanoTime

  val sink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(Paths.get(fileOut))

  val filesSource: Source[Path, NotUsed] = Directory.ls(submissionsDirectory).filter(p => p.getFileName.toString.startsWith("RS_"))

  val fileSink = FileIO.toPath(Paths.get(fileOut))
  val countSink = Sink.fold[Int, ByteString](0)((acc, _) => acc + 1)

  val (eventualResult, countResult) = filesSource
    .flatMapMerge(numberOfThreads, file => {
      println(file)

      getCompressorInputStreamSource(file.toString)
        .via(Flows.ndJsonToSubmission).async
        .via(Flows.objectToCsv)
    })
    .alsoToMat(fileSink)(Keep.right)
    .toMat(countSink)(Keep.both)
    .run()


  implicit val ec = system.dispatcher
  eventualResult.onComplete {
    case util.Success(_) => {
      println("Pipeline finished successfully.")
      completeAndTerminate()
    }
    case util.Failure(e) => {
      println(s"Pipeline failed with $e")
      completeAndTerminate()
    }
  }

  val count = Await.result(countResult, 365.days)
  println(f"Count: $count")



  def completeAndTerminate() ={
    val duration = (System.nanoTime - startTime) / 1e9d
    println(duration)

    system.terminate()
    val durationTerminated = (System.nanoTime - startTime) / 1e9d
    print("terminated after: ")
    println(durationTerminated)
  }

  @throws[FileNotFoundException]
  @throws[CompressorException]
  def getCompressorInputStreamSource(fileName: String): Source[ByteString, Future[IOResult]] = {
    val fileInputStream = new FileInputStream(new File(fileName))
    val bufferedInputStream = new BufferedInputStream(fileInputStream)
    val compressionName = CompressorStreamFactory.detect(bufferedInputStream)
    val compressorInputStream = new CompressorStreamFactory()
      .createCompressorInputStream(compressionName, bufferedInputStream, true)
    StreamConverters.fromInputStream(() => compressorInputStream)
  }
}
