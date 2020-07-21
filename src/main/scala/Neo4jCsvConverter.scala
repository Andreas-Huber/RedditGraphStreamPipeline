import java.io.{BufferedInputStream, File, FileInputStream, FileNotFoundException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Flow, Framing, JsonFraming, Sink, Source, StreamConverters}
import akka.util.ByteString
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}

import scala.concurrent.Future
import spray.json._
import DefaultJsonProtocol._
import akka.NotUsed
import com.typesafe.config.Optional
import model.Submission

import scala.collection.immutable
import akka.stream.alpakka.csv.scaladsl.{CsvFormatting, CsvQuotingStyle}

object Neo4jCsvConverter extends App{
  implicit val system = ActorSystem("ReadArchives")
  val fileIn = "C:\\import\\RS_v2_2008-03.gz"
  val fileOut = "C:\\import\\RS.out"

  // create the formats and provide them implicitly
  implicit val submissionFormat = jsonFormat2(Submission)

  val startTime = System.nanoTime



  val source: Source[ByteString, Future[IOResult]] = getCompressorInputStreamSource(fileIn)
  val sourceUc = FileIO.fromPath(Paths.get("C:\\import\\RS_2015-01"));

  val toCsvLine: Flow[immutable.Seq[String], ByteString, _] = CsvFormatting.format(
    CsvFormatting.Comma,
    CsvFormatting.DoubleQuote,
    CsvFormatting.Backslash,
    "\n",
    CsvQuotingStyle.Required,
    StandardCharsets.UTF_8,
    None)

  val sink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(Paths.get(fileOut))



  val eventualResult = source
    .via(Framing.delimiter( //chunk the inputs up into actual lines of text
      ByteString("\n"),
      maximumFrameLength = Int.MaxValue,
      allowTruncation = true))
//   .via(JsonFraming.objectScanner(Int.MaxValue))

    .map(_.utf8String)
    .map(_.parseJson.convertTo[Submission].toSeq)
    .via(toCsvLine)

    //.runForeach(i => print(i.utf8String))
    .runWith(sink)


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
