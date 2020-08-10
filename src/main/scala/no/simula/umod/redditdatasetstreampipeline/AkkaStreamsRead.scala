package no.simula.umod.redditdatasetstreampipeline

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.{Compression, Flow, Sink, Source, StreamConverters}
import akka.util.ByteString
import com.github.luben.zstd.ZstdInputStream

import scala.concurrent.Future

object AkkaStreamsRead extends App {
  implicit val system = ActorSystem("QuickStart")
  val filename = "G:\\temp\\RA_78M.csv.zst"
  val filein = "G:\\temp\\RA_78M.csv"
  val fileout = "D:\\temp\\out.ndjson.gz"

  val startTime = System.nanoTime

  val fileInputStream = new FileInputStream(new File(filename))
  val bufferedInputStream = new BufferedInputStream(fileInputStream)
  val zstdInputStream = new ZstdInputStream(bufferedInputStream)

  val gzInputStream = new FileInputStream(new File(filein))
  val outputStream = new FileOutputStream(new File(fileout))


  val source: Source[ByteString, Future[IOResult]] = StreamConverters.fromInputStream(() => zstdInputStream)

  //  Flow[ByteString].map(_.map(_.toChar.toUpper.toByte))
  val toGzip: Flow[ByteString, ByteString, NotUsed] = Compression.gzip

  val sink: Sink[ByteString, Future[IOResult]] = StreamConverters.fromOutputStream(() => outputStream)

  val eventualResult = source
    .via(toGzip.async)
    .runWith(sink)


  //  val result = scala.io.Source.fromInputStream(zstdInputStream).getLines().count(a => true);
  //  val result = scala.io.Source.from
  //  println("Lines: " + result)


  //  val source: Source[Int, NotUsed] = Source(1 to 100)
  //
  //  val done: Future[Done] = source.runForeach(i => println(i))


  implicit val ec = system.dispatcher
  eventualResult.onComplete(_ => {
    val duration = (System.nanoTime - startTime) / 1e9d
    println(duration)
    system.terminate()
  })
}