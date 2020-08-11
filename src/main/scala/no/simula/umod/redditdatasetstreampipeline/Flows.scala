package no.simula.umod.redditdatasetstreampipeline

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.stream.alpakka.csv.scaladsl.{CsvFormatting, CsvQuotingStyle}
import akka.stream.scaladsl.{Flow, Framing}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.model.{Submission, ToCsv}

object Flows {
//  // Takes a NdJson ByteStrings and spits them out as Submission objects
//  val ndJsonToObjectConverter : Flow[ByteString, Submission, NotUsed] = Flow[ByteString]
//    .via(Framing.delimiter( //chunk the inputs up into actual lines of text
//      ByteString("\n"),
//      maximumFrameLength = Int.MaxValue,
//      allowTruncation = true))
//    .map(_.utf8String.parseJson.convertTo[Submission]) // Create json objects

  val objectToCsvConverter : Flow[ToCsv, ByteString, NotUsed] = Flow[ToCsv]
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
