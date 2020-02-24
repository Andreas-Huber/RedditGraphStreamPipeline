import akka.stream._
import akka.stream.scaladsl._

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.util.ByteString
import scala.concurrent._
import scala.concurrent.duration._
import java.nio.file.Paths

object AkkaStreamsRead extends App{
  implicit val system = ActorSystem("QuickStart")
  val fileUrl = "https://reddittest81273.blob.core.windows.net/reddittest/Reddit_Subreddits.ndjson.zst"

  val source: Source[Int, NotUsed] = Source(1 to 100)

  val done: Future[Done] = source.runForeach(i => println(i))


  implicit val ec = system.dispatcher
  done.onComplete(_ => system.terminate())
}
