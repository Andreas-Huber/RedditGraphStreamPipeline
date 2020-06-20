import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}

import akka.stream._
import akka.stream.scaladsl._
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.util.ByteString

import scala.concurrent._
import scala.concurrent.duration._
import java.nio.file.Paths
import java.util.stream.IntStream

import Control.using
import ReadFile.filename
import com.github.luben.zstd.ZstdInputStream

object AkkaStreamsRead extends App{
  implicit val system = ActorSystem("QuickStart")
  val filename = "E:\\Shared drives\\Reddit\\subreddits\\Reddit_Subreddits.ndjson.zst"
  val fileout = "G:\\temp\\out.ndjson";


  val fileInputStream = new FileInputStream(new File(filename))
  val bufferedInputStream = new BufferedInputStream(fileInputStream)
  val zstdInputStream = new ZstdInputStream(bufferedInputStream)

  val outputStream = new FileOutputStream(new File(fileout))

//  var count = 0;



  val source: Source[ByteString, Future[IOResult]] = StreamConverters.fromInputStream(() => zstdInputStream)

  val toUpperCase: Flow[ByteString, ByteString, NotUsed] = Flow[ByteString].map(_.map(_.toChar.toUpper.toByte))

  val sink: Sink[ByteString, Future[IOResult]] = StreamConverters.fromOutputStream(() => outputStream)

  val eventualResult = source.via(toUpperCase).runWith(sink)

//  val result = scala.io.Source.fromInputStream(zstdInputStream).getLines().count(a => true);
//  val result = scala.io.Source.from
//  println("Lines: " + result)


//  val source: Source[Int, NotUsed] = Source(1 to 100)
//
//  val done: Future[Done] = source.runForeach(i => println(i))


  implicit val ec = system.dispatcher
  eventualResult.onComplete(_ => system.terminate())
}
