import java.io.{BufferedInputStream, File, FileInputStream, FileNotFoundException}
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Sink, Source, StreamConverters}
import akka.util.ByteString
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}

import scala.concurrent.Future

object ReadArchives extends App{
  implicit val system = ActorSystem("ReadArchives")
  val fileIn = "C:\\import\\RS.zst"
  val fileOut = "C:\\import\\RS.out"

  val startTime = System.nanoTime


  val source: Source[ByteString, Future[IOResult]] = getCompressorInputStreamSource(fileIn)

  val sink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(Paths.get(fileOut))

  val eventualResult = source
    .runWith(sink)


  implicit val ec = system.dispatcher
  eventualResult.onComplete(_ => {
    val duration = (System.nanoTime - startTime) / 1e9d
    println(duration)
    system.terminate()
  })


  @throws[FileNotFoundException]
  @throws[CompressorException]
  def getCompressorInputStreamSource(fileName: String): Source[ByteString, Future[IOResult]] = {
    val fileInputStream = new FileInputStream(new File(fileName))
    val bufferedInputStream = new BufferedInputStream(fileInputStream)
    val compressorInputStream = new CompressorStreamFactory().createCompressorInputStream(bufferedInputStream)
    StreamConverters.fromInputStream(() => compressorInputStream)
  }
}
