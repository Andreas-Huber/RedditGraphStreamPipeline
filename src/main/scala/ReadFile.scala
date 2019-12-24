import java.io.{File, FileInputStream}
import com.github.luben.zstd.{ZstdDirectBufferDecompressingStream, ZstdInputStream}
import scala.language.postfixOps
import scala.language.reflectiveCalls

object ReadFile extends App {
  val filename = "C:\\_\\ztest\\test.txt.zst"
  val fileInputStream = new FileInputStream(new File(filename))

  val in = new ZstdInputStream(fileInputStream)


  scala.io.Source.fromInputStream(in).getLines().foreach(println)

  in.close()
  fileInputStream.close()
}
