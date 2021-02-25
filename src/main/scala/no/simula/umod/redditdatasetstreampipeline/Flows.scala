package no.simula.umod.redditdatasetstreampipeline

import akka.NotUsed
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.IOResult
import akka.stream.Supervision.resumingDecider
import akka.stream.alpakka.csv.scaladsl.{CsvFormatting, CsvQuotingStyle}
import akka.stream.scaladsl.{Flow, Framing, Sink, Source, StreamConverters}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.model.JsonFormats._
import no.simula.umod.redditdatasetstreampipeline.model.ModelEntity.ModelEntity
import no.simula.umod.redditdatasetstreampipeline.model._
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}
import spray.json._

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileNotFoundException, FileOutputStream}
import java.nio.charset.StandardCharsets
import scala.concurrent.Future

object Flows {

  /**
   * Takes NdJson ByteStrings and converts them to the provided Entity
   */
  def ndJsonToObj(entity: ModelEntity) : Flow[ByteString, SubredditEntity, NotUsed] = {
    entity match {
      case ModelEntity.SubmissionEntity => splitLines.map(_.utf8String.parseJson.convertTo[Submission])
        .withAttributes(supervisionStrategy(resumingDecider))
      case ModelEntity.CommentEntity => splitLines.map(_.utf8String.parseJson.convertTo[Comment])
        .withAttributes(supervisionStrategy(resumingDecider))
      case ModelEntity.AuthorEntity => splitLines.map(_.utf8String.parseJson.convertTo[Author])
        .withAttributes(supervisionStrategy(resumingDecider))
      case ModelEntity.UserInSubredditEntity => splitLines.map(_.utf8String.parseJson.convertTo[UserInSubreddit])
        .withAttributes(supervisionStrategy(resumingDecider))
      case _ => throw new NotImplementedError("ndJson for this type is not implemented.")
    }
  }

  val splitLines: Flow[ByteString, ByteString, NotUsed] = Flow[ByteString]
    .via(Framing.delimiter( //chunk the inputs up into actual lines of text
      ByteString("\n"),
      maximumFrameLength = Int.MaxValue,
      allowTruncation = true))
    .filter(_.head != 0) // Remove lines with null bytes


  /**
   * Takes NdJson ByteStrings and converts them to Submission objects
   */
  val ndJsonToSubmission: Flow[ByteString, Submission, NotUsed] = Flow[ByteString]
    .via(splitLines)
    // Deserialize json to Submission
    .map(_.utf8String.parseJson.convertTo[Submission])
    .withAttributes(supervisionStrategy(resumingDecider))


  /**
   * Takes NdJson ByteStrings and converts them to Submission objects
   */
  val ndJsonToUserInSubreddit: Flow[ByteString, UserInSubreddit, NotUsed] = Flow[ByteString]
    .via(splitLines)
    // Deserialize json to UserInSubreddit
    .map(_.utf8String.parseJson.convertTo[UserInSubreddit])
    .withAttributes(supervisionStrategy(resumingDecider))


  /**
   * Converts an object to a line of CSV byte string
   */
  val objectToCsv: Flow[ToCsv, ByteString, NotUsed] = Flow[ToCsv]
    .map(s => s.toCsvSeq) // Get sequence of field values
    .via(CsvFormatting.format( // Create csv line
      CsvFormatting.Comma,
      CsvFormatting.DoubleQuote,
      CsvFormatting.Backslash,
      "\n",
      CsvQuotingStyle.Required,
      StandardCharsets.UTF_8,
      None))


  /**
   * Wraps a Java Compressor Stream into a Source for the given file.
   */
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

  /**
   * Wraps a Java Compressor Stream into a Source for the given file.
   */
  @throws[FileNotFoundException]
  @throws[CompressorException]
  def getCompressorStreamSink(fileName: File): Sink[ByteString, Future[IOResult]] = {
    val fileOutputStream = new FileOutputStream(fileName)
    val bufferedOutputStream = new BufferedOutputStream(fileOutputStream)

    val compressorInputStream = new CompressorStreamFactory()
      .createCompressorOutputStream(CompressorStreamFactory.ZSTANDARD, bufferedOutputStream)
    StreamConverters.fromOutputStream(() => compressorInputStream)
  }
}
