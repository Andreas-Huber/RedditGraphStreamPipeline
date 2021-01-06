package no.simula.umod.redditdatasetstreampipeline

import java.io.{BufferedInputStream, File, FileInputStream, FileNotFoundException}
import java.nio.charset.StandardCharsets
import akka.NotUsed
import akka.stream.IOResult
import akka.stream.alpakka.csv.scaladsl.{CsvFormatting, CsvQuotingStyle}
import akka.stream.scaladsl.{Flow, Framing, JsonFraming, Source, StreamConverters}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.model.JsonFormats._
import no.simula.umod.redditdatasetstreampipeline.model.ModelEntity.ModelEntity
import no.simula.umod.redditdatasetstreampipeline.model.{Author, Comment, ModelEntity, Submission, ToCsv}
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}
import spray.json._

import scala.concurrent.Future

object Flows {

  /**
   * Takes NdJson ByteStrings and converts them to the provided Entity
   */
  def ndJsonToObj(entity: ModelEntity) : Flow[ByteString, ToCsv, NotUsed] = {
    val f = Flow[ByteString]
     .via(Framing.delimiter( //chunk the inputs up into actual lines of text
       ByteString("\n"),
         maximumFrameLength = Int.MaxValue,
         allowTruncation = true))
      .filter(_.head != 0) // Remove lines with null bytes

    entity match {
      case ModelEntity.SubmissionEntity => f.map(_.utf8String.parseJson.convertTo[Submission])
      case ModelEntity.CommentEntity => f.map(_.utf8String.parseJson.convertTo[Comment])
      case ModelEntity.AuthorEntity => f.map(_.utf8String.parseJson.convertTo[Author])
      case _ => throw new NotImplementedError("ndJson for this type is not implemented.")
    }
  }

  /**
   * Takes NdJson ByteStrings and converts them to Submission objects
   */
  val ndJsonToSubmission: Flow[ByteString, Submission, NotUsed] = Flow[ByteString]
    .via(Framing.delimiter( //chunk the inputs up into actual lines of text
      ByteString("\n"),
      maximumFrameLength = Int.MaxValue,
      allowTruncation = true))
    .filter(_.head != 0) // Remove lines with null bytes
    .map(_.utf8String.parseJson.convertTo[Submission](submissionFormat)) // Deserialize json to Submission


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
}
