import java.io.{BufferedInputStream, File, FileInputStream}

import com.github.luben.zstd.ZstdInputStream

import scala.language.postfixOps
import scala.language.reflectiveCalls
import Control._

object ReadFile extends App {
  //val filename = "//media//andreas//Andal//reddit//Reddit_Subreddits.ndjson.zst"
  val filename = "C:\\_\\ztest\\Reddit_Subreddits.ndjson.zst"


  val startedAtNanos = System.nanoTime()


  using(new FileInputStream(new File(filename))) { fileInputStream => {
    using(new BufferedInputStream(fileInputStream)) { bufferedInputStream => {
      using(new ZstdInputStream(fileInputStream)) { zstdInputStream => {


        var a: Int = 0;
        while({
          a = zstdInputStream.available()
          a > 0}) {
          zstdInputStream.readNBytes(a);
        }


//        var count = 0;
//        val result = scala.io.Source.fromInputStream(zstdInputStream).getLines().count(a => true);
//        println("Lines: " + result)
      }}
    }}
  }}

  println("Seconds: " + (System.nanoTime() - startedAtNanos) / 1_000_000_000)
}


/*
time zstd -t (linux ubuntu)

Reddit_Subreddits.ndjson.zst: 78688602372 bytes

real	0m31,697s
user	0m30,284s
sys	0m1,412s
---------------------------
time zstd -t (WSL ubuntu)
Reddit_Subreddits.ndjson.zst: 78688602372 bytes

real    0m46.030s
user    0m33.578s
sys     0m12.391s
---------------------------
Input stream linux
Lines: 49723295
Seconds: 183

---------------------------
Buffered input stream linux
Seconds: 185

---------------------------
Input stream windows
Seconds: 191



---------------------------
Buffered Input stream windows
Seconds: 46
 */