import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}

import org.apache.commons.compress.compressors.{CompressorInputStream, CompressorStreamFactory}
import Control._

import scala.io.Source
import scala.language.reflectiveCalls
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream

object ReadFile extends App {
  val filename = "D:\\temp\\test.txt"

  val fileInputStream = new FileInputStream(new File(filename))
  val in = new BufferedInputStream(fileInputStream)

  val out = new FileOutputStream(new File("D:\\temp\\out.txt"))

  val zsIn = new ZstdCompressorInputStream(in)
  val buffer = new Array[Byte](4096)
  var n = 0
  while ( {
    -1 != (n = zsIn.read(buffer))
  }) out.write(buffer, 0, n)
  out.close

}

//object ReadFile extends App {
//  val filename = "D:\\temp\\test.txt"
//
//  val fileInputStream = new FileInputStream(new File(filename))
//
//  using(Source.fromFile(filename)) { bufferedSource => {
//    val zis   = new ZstdInputStream (is)
//
//    for (line <- bufferedSource.getLines) {
//      println(line.toUpperCase)
//    }
//  }}
//}