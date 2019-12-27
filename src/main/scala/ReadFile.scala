import java.io.{BufferedInputStream, File, FileInputStream}

import com.github.luben.zstd.ZstdInputStream

import scala.language.postfixOps
import scala.language.reflectiveCalls
import Control._

object ReadFile extends App {
  val filename = "C:\\_\\ztest\\Reddit_Subreddits.ndjson.zst"
  // val filename = "C:\\_\\ztest\\test.txt.zst"


  val startedAtNanos = System.nanoTime()


  using(new FileInputStream(new File(filename))) { fileInputStream => {
    using(new BufferedInputStream(fileInputStream)) { bufferedInputStream => {
      using(new ZstdInputStream(fileInputStream)) { zstdInputStream => {
        var count = 0;
        val result = scala.io.Source.fromInputStream(zstdInputStream).getLines().count(a => true);
        println("Lines: " + result)
      }}
    }}
  }}

  println("Milliseconds: " + (System.nanoTime() - startedAtNanos) / 1_000_000)
}
