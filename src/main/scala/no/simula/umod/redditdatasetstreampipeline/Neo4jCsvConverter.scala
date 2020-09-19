package no.simula.umod.redditdatasetstreampipeline

import java.io.{BufferedInputStream, File, FileInputStream, FileNotFoundException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.actor.Status.{Failure, Success}
import akka.stream.IOResult
import akka.stream.alpakka.csv.scaladsl.{CsvFormatting, CsvQuotingStyle}
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{FileIO, Flow, Framing, Keep, Sink, Source, StreamConverters}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.model.{Submission, ToCsv}
import no.simula.umod.redditdatasetstreampipeline.model.JsonFormats._
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}
import spray.json._
import scala.util.{Failure, Success}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

object Neo4jCsvConverter extends App {
  implicit val system = ActorSystem("ReadArchives")
  val fileIn = "C:\\import\\RS_v2_2008-03.gz"
  val fileOut = "C:\\import\\_RS.out"
  val submissionsDirectory = Paths.get("C:\\import\\submissions\\");
  val numberOfThreads = 6;


  val startTime = System.nanoTime

  val sink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(Paths.get(fileOut))

  val filesSource: Source[Path, NotUsed] = Directory.ls(submissionsDirectory).filter(p => p.getFileName.toString.startsWith("RS_"))

  val fileSink = FileIO.toPath(Paths.get(fileOut))
  val countSink = Sink.fold[Int, ByteString](0)((acc, _) => acc + 1)

  val (eventualResult) = filesSource
    .flatMapMerge(numberOfThreads, file => {
      println(file)

      getCompressorInputStreamSource(file.toString)
        .via(Flows.ndJsonToSubmission).async
        .via(Flows.objectToCsv)
    })
    .alsoToMat(fileSink)(Keep.right)
//    .toMat(countSink)(Keep.both)
    .run()


  //
  //    val eventualResult : Future[Done] = filesSource
  //        .mapAsync(1)(p =>
  //          {
  //
  //            val fileName = p.getFileName;
  //            val outDir = p.getParent;
  //            val out = outDir + "\\out\\" + fileName + ".out"
  //
  //            println(out)
  //
  //            val  blub : Future[IOResult] =  getCompressorInputStreamSource(p.toString)
  //                .via(ndJsonToCsvConverter).async
  //                .runWith(FileIO.toPath(Paths.get(out)))
  //            blub
  //
  //          })
  ////      .recover {
  ////        case e => throw e
  ////      }
  //      .runForeach(f => println(f.count))

//  countResult.onComplete {
//    case scala.util.Success(_:Int) =>
//      println("Stream finished successfully.")
//    case scala.util.Failure(e) =>
//      println(s"Stream failed with $e")
//  }

//  countResult.onComplete(_ => println(s"CountResult:"))



  implicit val ec = system.dispatcher
  eventualResult.onComplete {
    case scala.util.Success(_) => {
      println("Pipeline finished successfully.")
      completeAndTerminate()
    }
    case scala.util.Failure(e) => {
      println(s"Pipeline failed with $e")
      completeAndTerminate()
    }
  }

//  eventualResult.onComplete(_ => {
//    val duration = (System.nanoTime - startTime) / 1e9d
//    println(duration)
//
//    system.terminate()
//    val durationTerminated = (System.nanoTime - startTime) / 1e9d
//    print("terminated after: ")
//    println(durationTerminated)
//  })


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
    val compressorInputStream = new CompressorStreamFactory().createCompressorInputStream(bufferedInputStream)
    StreamConverters.fromInputStream(() => compressorInputStream)
  }
}
