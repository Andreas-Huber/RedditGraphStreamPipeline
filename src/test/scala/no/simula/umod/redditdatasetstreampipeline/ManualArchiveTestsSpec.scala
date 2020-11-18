package no.simula.umod.redditdatasetstreampipeline

import java.io.{BufferedInputStream, FileInputStream, FileOutputStream, InputStream}
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Keep, Sink}
import no.simula.umod.redditdatasetstreampipeline.model.ToCsv
import org.scalactic.source.Position
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatest.{BeforeAndAfter, Ignore}
import org.tukaani.xz.{SingleXZInputStream, XZInputStream}

import scala.concurrent.Await

@Ignore // Only for manual bug search
class ManualArchiveTestsSpec extends AnyFlatSpec with BeforeAndAfter {
    implicit val system: ActorSystem = ActorSystem("Test")

    override protected def after(fun: => Any)(implicit pos: Position): Unit = system.terminate()

  "jsonParser" should "be able to parse the whole file without compression" in {
    val fileSource = FileIO.fromPath(Paths.get("C:\\_\\RS_v2_2008-03"))

    val doNothingSink = Sink.foreach(DoNothing.doNothing)
    val countSink = Sink.fold[Int, ToCsv](0)((acc, _) => acc + 1)


    val (result, num) = fileSource
      .via(Flows.ndJsonToSubmission)
      .alsoToMat(doNothingSink)(Keep.right)
      .toMat(countSink)(Keep.both)
      .run()

    Await.result(result, 300.seconds)
    val numb = Await.result(num, 300.seconds)

    assert(numb == 168227)
    println(numb)

  }

  "jsonParser" should "be able to parse the whole compressed file" in {
    val path = "C:\\import\\RS_v2_2008-03.xz"
    val compSource = Flows.getCompressorInputStreamSource(path)

    val doNothingSink = Sink.foreach(DoNothing.doNothing)
    val countSink = Sink.fold[Int, ToCsv](0)((acc, _) => acc + 1)


    val (result, num) = compSource
      .via(Flows.ndJsonToSubmission)
      .alsoToMat(doNothingSink)(Keep.right)
      .toMat(countSink)(Keep.both)
      .run()

    Await.result(result, 300.seconds)
    val numb = Await.result(num, 300.seconds)

    assert(numb == 168227)
    println(numb)

  }

  "xzip" should "be able to decompress the whole compressed file" in {
    val input = "C:\\import\\RS_v2_2008-03.xz"
    val output = "C:\\_\\RS_v2_2008-03.out.ndjson"

    val compSource = Flows.getCompressorInputStreamSource(input)



    val fileSink = FileIO.toPath(Paths.get(output))

    val resultFuture = compSource
      .runWith(fileSink)

    Await.result(resultFuture, 300.seconds)

    // Remarks: This fails - wc of this file is lower than the original
    //   168227   1458212 239138694 RS_v2_2008-03
    //   141620   1232752 201330688 RS_v2_2008-03.out.ndjson
  }

  "XZInputStream" should "be able to decompress with java steams" in {
    val input = "C:\\import\\RS_v2_2008-03.xz"
    val output = "C:\\_\\RS_v2_2008-03.outjava"
    val inputStream : InputStream = new FileInputStream(input)
    val xzInputStream : XZInputStream = new XZInputStream(inputStream)
    val outputStream = new FileOutputStream(output)

    xzInputStream.transferTo(outputStream)

    xzInputStream.close()
    inputStream.close()
    outputStream.close()

  }

  "XZInputStream" should "be able to decompress with java buffered input steam" in {
    val input = "C:\\import\\RS_v2_2008-03.xz"
    val output = "C:\\_\\RS_v2_2008-03.outjavabuff"
    val inputStream : InputStream = new FileInputStream(input)
    val bufferedInputStream = new BufferedInputStream(inputStream)
    val xzInputStream : XZInputStream = new XZInputStream(bufferedInputStream)
    val outputStream = new FileOutputStream(output)

    xzInputStream.transferTo(outputStream)

    xzInputStream.close()
    bufferedInputStream.close()
    inputStream.close()
    outputStream.close()

  }

  "SingleXZInputStream" should "be able to decompress with java buffered input steam" in {
    val input = "C:\\import\\RS_v2_2008-03.xz"
    val output = "C:\\_\\RS_v2_2008-03.outjavaxzsingle"
    val inputStream : InputStream = new FileInputStream(input)
    val bufferedInputStream = new BufferedInputStream(inputStream)
    val singleXzInputStream = new SingleXZInputStream(bufferedInputStream)
    val outputStream = new FileOutputStream(output)

    singleXzInputStream.transferTo(outputStream)

    singleXzInputStream.close()
    bufferedInputStream.close()
    inputStream.close()
    outputStream.close()

    // SingleXzInputStream has issues and stops
  }
}

object DoNothing{
  //noinspection ScalaUnusedSymbol
  def doNothing(x : Any): Unit ={

  }
}
