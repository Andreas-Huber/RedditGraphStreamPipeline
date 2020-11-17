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

    val expected = Submission(Option("SR"), Option("a"), Option("author"), Option("title"))
    val json = """{"id":"a","subreddit":"SR","author":"author","title":"title"}"""


    val result = Source.single(ByteString(json))
      .via(conv)
      .runWith(Sink.seq)

    val res = Await.result(result, 3.seconds);
    assert(res(0) === expected)
    assert(res.length === 1)
  }

  "ndJsonToSubmission" should "be able to deserialize a line from the reddit submissions dataset" in {
    val conv = Flows.ndJsonToSubmission

    val expected = Submission(Option("Jeff_Is_Fat"), Option("6dis8"), Option("[deleted]"),  Option("Jeff is fat!"))
    val json = """{"archived":true,"author":"[deleted]","author_flair_background_color":"","author_flair_css_class":null,"author_flair_text":null,"author_flair_text_color":"dark","brand_safe":false,"can_gild":false,"contest_mode":false,"created_utc":1206577677,"distinguished":null,"domain":"self.reddit.com","edited":false,"gilded":0,"hidden":false,"hide_score":false,"id":"6dis8","is_crosspostable":false,"is_reddit_media_domain":false,"is_self":false,"is_video":false,"link_flair_css_class":null,"link_flair_richtext":[],"link_flair_text":null,"link_flair_text_color":"dark","link_flair_type":"text","locked":false,"media":null,"media_embed":{},"no_follow":true,"num_comments":0,"num_crossposts":0,"over_18":false,"parent_whitelist_status":null,"permalink":"\/r\/Jeff_Is_Fat\/comments\/6dis8\/jeff_is_fat\/","retrieved_on":1522687818,"rte_mode":"markdown","score":1,"secure_media":null,"secure_media_embed":{},"selftext":"[deleted]","send_replies":true,"spoiler":false,"stickied":false,"subreddit":"Jeff_Is_Fat","subreddit_id":"t5_2qhnz","subreddit_name_prefixed":"r\/Jeff_Is_Fat","subreddit_type":"public","suggested_sort":null,"thumbnail":"default","thumbnail_height":null,"thumbnail_width":null,"title":"Jeff is fat!","url":"http:\/\/self.reddit.com","whitelist_status":null}"""


    val result = Source.single(ByteString(json))
      .via(conv)
      .runWith(Sink.seq)

    val res = Await.result(result, 3.seconds);
    assert(res(0) === expected)
    assert(res.length === 1)

  }

  "objectToCsv" should "should convert ToCsv traits to CSV ByteStrings" in {
    val conv = Flows.objectToCsv

    val result = Source.single(Submission(Option("Cats"), Option("555"), Option("ALF"),  Option("Melmacs-best-cat-recipes.")))
      .via(conv)
      .map(_.utf8String)
      .runWith(Sink.seq)

    val res = Await.result(result, 3.seconds);
    assert(res(0) === "Cats,555,ALF,Melmacs-best-cat-recipes.\n");

  }


}