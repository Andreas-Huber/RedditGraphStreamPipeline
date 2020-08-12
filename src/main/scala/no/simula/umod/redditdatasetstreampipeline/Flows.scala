package no.simula.umod.redditdatasetstreampipeline

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.stream.alpakka.csv.scaladsl.{CsvFormatting, CsvQuotingStyle}
import akka.stream.scaladsl.{Flow, Framing}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.model.{Submission, ToCsv}
import spray.json._
import no.simula.umod.redditdatasetstreampipeline.model.JsonFormats._

object Flows {

  /**
  * Takes NdJson ByteStrings and converts them to Submission objects
  */
  val ndJsonToSubmission : Flow[ByteString, Submission, NotUsed] = Flow[ByteString]
    .via(Framing.delimiter( //chunk the inputs up into actual lines of text
      ByteString("\n"),
      maximumFrameLength = Int.MaxValue,
      allowTruncation = true))
    .map(_.utf8String.parseJson.convertTo[Submission]) // Create json objects

    /**
     * Converts an object to a line of CSV byte string
     */
    val objectToCsv : Flow[ToCsv, ByteString, NotUsed] = Flow[ToCsv]
    .map(s => s.toCsvSeq)      // Get sequence of field values
    .via(CsvFormatting.format( // Create csv line
      CsvFormatting.Comma,
      CsvFormatting.DoubleQuote,
      CsvFormatting.Backslash,
      "\n",
      CsvQuotingStyle.Required,
      StandardCharsets.UTF_8,
      None))
}
