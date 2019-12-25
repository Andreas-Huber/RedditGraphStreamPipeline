import java.io.{File, FileInputStream}
import com.github.luben.zstd.ZstdInputStream
import scala.language.postfixOps
import scala.language.reflectiveCalls
import Control._

object ReadFile extends App {
  val filename = "C:\\_\\ztest\\test.txt.zst"

  using(new FileInputStream(new File(filename))) { fileInputStream =>{
    using(new ZstdInputStream(fileInputStream)) { in => {
      scala.io.Source.fromInputStream(in).getLines().foreach(println)
    }}
  }}
}
