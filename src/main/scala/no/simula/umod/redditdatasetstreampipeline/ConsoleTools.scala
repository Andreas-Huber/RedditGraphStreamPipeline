package no.simula.umod.redditdatasetstreampipeline

import java.text.SimpleDateFormat
import java.util.Calendar

object ConsoleTools {

  private def getMemoryString() : String= {
    val gb = 1024*1024*1024
    val runtime = Runtime.getRuntime
    val memory : String =
      f"U:${(runtime.totalMemory - runtime.freeMemory) / gb}" +
      f"T:${ runtime.totalMemory / gb}" +
      f"M:${runtime.maxMemory / gb}"
    memory
  }

  /**
   * Logg the message with including the time
   * @param message message to log
   */
  def log(message: Any) = {
    println(s"$getDateString  $message")
  }

  /**
   * Logg the message with including the time and duration
   *
   * @param message message to log
   * @param startNanoTime start time in nano seconds (usually from System.nanoTime())
   */
  def logDuration(message: Any, startNanoTime: Long) = {

    val duration = (System.nanoTime - startNanoTime) / 1e9d

    println(f"$getDateString  $message ($duration%1.1f seconds) ${getMemoryString()}")
  }

  /** Get nicely formatted string of the current time */
  private def getDateString = {
    val date = Calendar.getInstance().getTime()
    val dateFormat = new SimpleDateFormat("MMM dd HH:mm:ss")
    val dateString = dateFormat.format(date)
    dateString
  }
}
