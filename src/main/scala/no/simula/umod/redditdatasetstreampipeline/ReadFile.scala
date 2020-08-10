package no.simula.umod.redditdatasetstreampipeline

import java.io.{BufferedInputStream, File, FileInputStream}

import com.github.luben.zstd.ZstdInputStream
import no.simula.umod.redditdatasetstreampipeline.Control.using

object ReadFile extends App {

  //val filename = "//home//andreas//redditcache/subreddits//Reddit_Subreddits.ndjson.zst"
  val filename = "E:\\Shared drives\\Reddit\\subreddits\\Reddit_Subreddits.ndjson.zst"
  //val fileUrl = "https://reddittest81273.blob.core.windows.net/reddittest/Reddit_Subreddits.ndjson.zst"

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
      }
      }
    }
    }
  }
  }

  println("Seconds: " + (System.nanoTime() - startedAtNanos) / 1_000_000_000)
}
