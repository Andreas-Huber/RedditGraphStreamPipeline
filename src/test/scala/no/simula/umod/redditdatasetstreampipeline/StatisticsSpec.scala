package no.simula.umod.redditdatasetstreampipeline

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import no.simula.umod.redditdatasetstreampipeline.model.UserInSubreddit
import org.scalactic.source.Position
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import scala.concurrent.Await

class StatisticsSpec extends AnyFlatSpec with BeforeAndAfter {
  private implicit val system: ActorSystem = ActorSystem("Test")

  override protected def after(fun: => Any)(implicit pos: Position): Unit = system.terminate()

  "countUsersInSubredditsPerFile" should "should label subreddits correctly" in {
    val stat = new Statistics(system, Config())

    val result = Source.single(UserInSubreddit(Option("AskReddit"), Option("Hans")))
      .via(stat.countUsersInSubredditsPerFile)
      .runWith(Sink.seq)

    val res = Await.result(result, 3.seconds)
    assert(res.head.count === 1)
    assert(res.head.subreddit === "AskReddit")
    assert(res.length === 1)
  }

  "countUsersInSubredditsPerFile" should "should ignore multiple user entries per subreddit correctly" in {
    val stat = new Statistics(system, Config())

    val source = Source(
      UserInSubreddit(Option("AskReddit"), Option("Hans")) ::
        UserInSubreddit(Option("Test"), Option("Hans")) ::
        UserInSubreddit(Option("AskReddit"), Option("Hans")) ::
        UserInSubreddit(Option("Test"), Option("Hans")) ::
        UserInSubreddit(Option("AskReddit"), Option("Jaqueline")) ::
        UserInSubreddit(Option("Test"), Option("Hans")) ::
        UserInSubreddit(Option("AskReddit"), Option("Sepp")) ::
        UserInSubreddit(Option("AskReddit"), Option("Hans")) ::
        UserInSubreddit(Option("Test"), Option("Schorsch")) ::
        UserInSubreddit(Option("AskReddit"), Option("Hans")) ::
        UserInSubreddit(Option("reddit.com"), Option("Hans")) ::
        Nil)

    val result = source
      .via(stat.countUsersInSubredditsPerFile)
      .runWith(Sink.seq)

    val res = Await.result(result, 3.seconds).toList
    assert(res.length === 3, "there should be three subreddits")

    val askReddit = res.find(c => c.subreddit === "AskReddit")
    assert(askReddit.get.count === 3)

    val test = res.find(c => c.subreddit === "Test")
    assert(test.get.count === 2)

    val redditCom = res.find(c => c.subreddit === "reddit.com")
    assert(redditCom.get.count === 1)
  }
}
