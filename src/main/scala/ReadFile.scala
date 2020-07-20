import java.io.{BufferedInputStream, File, FileInputStream}
import java.net.URL

import com.github.luben.zstd.ZstdInputStream

import scala.language.postfixOps
import scala.language.reflectiveCalls
import Control._

object ReadFile extends App {

  //val filename = "//home//andreas//redditcache/subreddits//Reddit_Subreddits.ndjson.zst"
  val filename = "E:\\Shared drives\\Reddit\\subreddits\\Reddit_Subreddits.ndjson.zst"
  //val fileUrl = "https://reddittest81273.blob.core.windows.net/reddittest/Reddit_Subreddits.ndjson.zst"

  import java.io.InputStream

  //new URL(fileUrl).openStream
  //new FileInputStream(new File(filename))
  val startedAtNanos = System.nanoTime()


  using(new FileInputStream(new File(filename))) { fileInputStream => {
    using(new BufferedInputStream(fileInputStream)) { bufferedInputStream => {
      using(new ZstdInputStream(bufferedInputStream)) { zstdInputStream => {
        var count = 0;

//        while(zstdInputStream.transferTo())

          val result = scala.io.Source.fromInputStream(zstdInputStream).getLines().count(a => true);
          println("Lines: " + result)
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