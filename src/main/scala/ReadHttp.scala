import Control.using
import com.github.luben.zstd.ZstdInputStream

object ReadHttp extends App{

  val startedAtNanos = System.nanoTime()

  using(getHttpStream("https://aorj62hzsyxnpmmm.myfritz.net:48340/nas/filelink.lua?id=53b696b0476d25da")) {inputStream => {
    using(new ZstdInputStream(inputStream)) { zstdInputStream => {
      var transferred = 0L
      var read = 0
      val buffer = new Array[Byte](8192)

      while ({
        read = zstdInputStream.read(buffer, 0, 8192)
        read >= 0
      }) {
        transferred += read.toLong
      }
    }}
  }}

  println("Http Zstd took seconds: " + (System.nanoTime() - startedAtNanos) / 1_000_000_000)


  @throws(classOf[java.io.IOException])
  @throws(classOf[java.net.SocketTimeoutException])
  def getHttpStream(url: String,
          connectTimeout: Int = 10000,
          readTimeout: Int = 0,
          requestMethod: String = "GET")  =
  {
    import java.net.{URL, HttpURLConnection}
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    connection.getInputStream
  }
}
