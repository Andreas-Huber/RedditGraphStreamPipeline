import java.io.{File, FileOutputStream}
import java.nio.file.Paths

import AkkaStreamsRead.{eventualResult, fileout, outputStream, system}
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Sink, Source, StreamConverters}
import akka.util.ByteString

import scala.concurrent.Future

object AkkaStreamsZstdByteBuffer extends App{
  implicit val system = ActorSystem("ByteBufferTest")
  val filename = "G:\\temp\\RA_78M.csv.ultra.gz"
  val fileout = "D:\\temp\\out.bytebuff"

  val startTime = System.nanoTime

  val outputStream = new FileOutputStream(new File(fileout))

  val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(Paths.get(filename))

  val sink: Sink[ByteString, Future[IOResult]] = StreamConverters.fromOutputStream(() => outputStream)


  val eventualResult = source
    .runWith(sink)


  implicit val ec = system.dispatcher
  eventualResult.onComplete(_ => {
    val duration = (System.nanoTime - startTime) / 1e9d
    println(duration)
    system.terminate()
  })
}
