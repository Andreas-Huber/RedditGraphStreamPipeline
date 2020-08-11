package no.simula.umod.redditdatasetstreampipeline

import java.io.{BufferedInputStream, File, FileInputStream, FileNotFoundException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.csv.scaladsl.{CsvFormatting, CsvQuotingStyle}
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{FileIO, Flow, Framing, Sink, Source, StreamConverters}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.model.{Submission, ToCsv}
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.Future

object Neo4jCsvConverter extends App {
  implicit val system = ActorSystem("ReadArchives")
  val fileIn = "C:\\import\\RS_v2_2008-03.gz"
  val fileOut = "C:\\import\\RS.out"
  val submissionsDirectory = Paths.get("C:\\import\\submissions\\");

  // create the formats and provide them implicitly
  implicit val submissionFormat = jsonFormat2(Submission)

  val startTime = System.nanoTime


  val source: Source[ByteString, Future[IOResult]] = getCompressorInputStreamSource(fileIn)

  val sink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(Paths.get(fileOut))



  // Takes a NdJson ByteStrings and spits them out as CSV ByteStrings
  val ndJsonToCsvConverter = Flow[ByteString]
    .via(Framing.delimiter( //chunk the inputs up into actual lines of text
      ByteString("\n"),
      maximumFrameLength = Int.MaxValue,
      allowTruncation = true))
    .map(_.utf8String.parseJson.convertTo[Submission].toCsvSeq) // Create json objects and then sequences of strings
    .via(CsvFormatting.format( // Create csv line
      CsvFormatting.Comma,
      CsvFormatting.DoubleQuote,
      CsvFormatting.Backslash,
      "\n",
      CsvQuotingStyle.Required,
      StandardCharsets.UTF_8,
      None))


  val filesSource: Source[Path, NotUsed] = Directory.ls(submissionsDirectory).filter(p => p.getFileName.toString.startsWith("RS_"))


  val eventualResult = filesSource
    .flatMapConcat(file => {
      println(file)

      getCompressorInputStreamSource(file.toString)
        .via(ndJsonToCsvConverter).async
    })
    .runWith(FileIO.toPath(Paths.get(fileOut)))


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


  //  val sinkinger : Sink[Any, NotUsed] = Sink.cancelled;
  //
  //  val fl :  Flow[Path, ByteString, NotUsed] = Flow.fromSinkAndSource(sinkinger, getCompressorInputStreamSource(_));


  //
  //  val eventualResult : Future[Seq[Future[IOResult]]] = filesSource
  //      .map(p =>
  //        {
  //          println(p.toString)
  //          val subResult = Source.single(
  //            getCompressorInputStreamSource(p.toString)
  //              .via(ndJsonToCsvConverter)
  //              .runWith(FileIO.toPath(Paths.get(fileOut)))
  //          )
  //        })
  //    .runWith(Sink.seq)


  //    .via(ndJsonToCsvConverter)


  //    .runForeach(i => println(i))
  //.runWith(sink)


  implicit val ec = system.dispatcher
  eventualResult.onComplete(_ => {
    val duration = (System.nanoTime - startTime) / 1e9d
    println(duration)

    system.terminate()
  })


  @throws[FileNotFoundException]
  @throws[CompressorException]
  def getCompressorInputStreamSource(fileName: String): Source[ByteString, Future[IOResult]] = {
    val fileInputStream = new FileInputStream(new File(fileName))
    val bufferedInputStream = new BufferedInputStream(fileInputStream)
    val compressorInputStream = new CompressorStreamFactory().createCompressorInputStream(bufferedInputStream)
    StreamConverters.fromInputStream(() => compressorInputStream)
  }
}
