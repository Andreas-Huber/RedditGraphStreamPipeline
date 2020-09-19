package no.simula.umod.redditdatasetstreampipeline

import java.nio.charset.StandardCharsets

import akka.NotUsed
import akka.stream.{FlowShape, IOResult}
import akka.stream.alpakka.csv.scaladsl.{CsvFormatting, CsvQuotingStyle}
import akka.stream.scaladsl.{Balance, Flow, Framing, GraphDSL, JsonFraming, Merge, Source}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.model.JsonFormats._
import no.simula.umod.redditdatasetstreampipeline.model.{Submission, ToCsv}
import spray.json._

import scala.concurrent.Future

object Flows {

  /**
   * Takes NdJson ByteStrings and converts them to Submission objects
   */
  val ndJsonToSubmission: Flow[ByteString, ToCsv, NotUsed] = Flow[ByteString]
    .via(Framing.delimiter( //chunk the inputs up into actual lines of text
      ByteString("\n"),
      maximumFrameLength = Int.MaxValue,
      allowTruncation = true))
//    .via(JsonFraming.objectScanner(Int.MaxValue))
    .map(_.utf8String.parseJson.convertTo[Submission]) // Create json objects

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
   * Parallel flat map concat
   * Source: https://stackoverflow.com/questions/46972340/how-to-do-a-parallel-flatmapconcat-in-akka-streams
   *
   * @param parallelism
   * @param sourceCreator
   * @tparam I
   * @tparam O
   * @return
   */
  def parallelFlatMapConcat[I, O](parallelism: Int)(sourceCreator: I => Source[O, Future[IOResult]]) = {
    val mapConcat = Flow[I].flatMapConcat(sourceCreator)

    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val balance = builder.add(Balance[I](parallelism))
      val merge = builder.add(Merge[O](parallelism))

      for (i ← 0 until parallelism)
        balance.out(i) ~> mapConcat ~> merge.in(i)

      FlowShape(balance.in, merge.out)
    }
  }
}
