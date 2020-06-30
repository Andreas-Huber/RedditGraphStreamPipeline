import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.file.Paths

import AkkaStreamsRead.{eventualResult, fileout, outputStream, system}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source, StreamConverters}
import akka.util.ByteString
import com.github.luben.zstd.Zstd

import scala.concurrent.Future

object AkkaStreamsZstdByteBuffer extends App{
  implicit val system = ActorSystem("ByteBufferTest")
  val filename = "G:\\temp\\RA.sample.csv"
  val fileout = "D:\\temp\\out.bytebuff"

  val startTime = System.nanoTime

  val outputStream = new FileOutputStream(new File(fileout))

  val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(Paths.get(filename))

  val sink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(Paths.get(fileout))

  val decompressZstd: Flow[ByteString, ByteString, NotUsed] = Flow[ByteString].map(byteString => {
    val buffer = ByteBuffer.allocate(byteString.length)
    Zstd.compress(buffer, byteString.asByteBuffer)
    ByteString.fromByteBuffer(buffer)
  })

  val eventualResult = source
    .via(decompressZstd)
    .runWith(sink)


  implicit val ec = system.dispatcher
  eventualResult.onComplete(_ => {
    val duration = (System.nanoTime - startTime) / 1e9d
    println(duration)
    system.terminate()
  })
}
