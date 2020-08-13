package no.simula.umod.redditdatasetstreampipeline

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import no.simula.umod.redditdatasetstreampipeline.model.Submission
import org.scalactic.source.Position
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import scala.concurrent.Await

class FlowsSpec extends AnyFlatSpec with BeforeAndAfter {
  implicit val system = ActorSystem("Test")

  override protected def after(fun: => Any)(implicit pos: Position): Unit = system.terminate()

  "ndJsonToSubmission" should "should convert json byte strings to Submission objects" in {
    val conv = Flows.ndJsonToSubmission

    val expected = Submission(Option("SR"), Option("a"))
    val json = "{\"id\":\"a\",\"subreddit\":\"SR\"}\n"


    val result = Source.single(ByteString(json))
      .via(conv)
      .runWith(Sink.seq)

    val res = Await.result(result, 3.seconds);
    assert(res(0) === expected)
    assert(res.length === 1)
  }

  "objectToCsv" should "should convert ToCsv traits to CSV ByteStrings" in {
    val conv = Flows.objectToCsv

    val result = Source.single(new Submission(Option("SR"), Option("555")))
      .via(conv)
      .map(_.utf8String)
      .runWith(Sink.seq)

    val res = Await.result(result, 3.seconds);
    assert(res(0) === "SR,555\n");

  }
}