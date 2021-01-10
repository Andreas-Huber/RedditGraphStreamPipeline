package no.simula.umod.redditdatasetstreampipeline

import java.text.SimpleDateFormat
import java.util.Calendar

object ConsoleTools {

  /**
   * Logg the message with including the time
   * @param path
   */
  def log(message: Any) = {

    val date =  Calendar.getInstance().getTime()
    val dateFormat = new SimpleDateFormat("MMM dd HH:mm:ss")
    val dateString = dateFormat.format(date)

    println(s"$dateString  $message")
  }
}
