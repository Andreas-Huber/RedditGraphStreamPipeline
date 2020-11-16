package no.simula.umod.redditdatasetstreampipeline

import java.io.{BufferedInputStream, File, FileInputStream, FileNotFoundException}
import java.nio.file.{Path, Paths}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source, StreamConverters}
import akka.util.ByteString
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.sys.exit

object Main extends App {

  import scopt.OParser

  val builder = OParser.builder[Config]
  val parser1 = {
    import builder._
    OParser.sequence(
      programName("redditdatasetstreampipeline"),
      head("Reddit Dataset Stream Pipeline", "0.1"),

      opt[File]('d', "dataset-dir")
        .valueName("<directory>")
        .action((x, c) => c.copy(datasetDirectory = x))
        .text("Dataset directory that contains the submissions and comments folder. Default value: 'redditdataset'."),

      opt[Int]('p', "concurrent-files")
        .valueName("<number 1 to n>")
        .action((x, c) => c.copy(numberOfConcurrentFiles = x))
        .validate(x =>
          if (x > 0) success
          else failure("Value <number> must be >0"))
        .text("Number of how many files should be read concurrently."),

      help("help").text("prints this usage text"),

      cmd("passtrough")
        .action((_, c) => c.copy(programMode = ProgramMode.PassTrough))
        .text("Reads the datasets files and provides them as a output stream.")
        .children(
          opt[Unit]('s', "submissions")
            .action((_, c) => c.copy(provideSubmissionsStream = true))
            .text("Enables the submission output stream."),

          opt[Unit]('c', "comments")
            .action((_, c) => c.copy(provideCommentsStream = true))
            .text("Enables the comments output stream."),

          opt[File]('x', "submission-out")
            .valueName("<file>")
            .action((x, c) => c.copy(submissionsOutFile = x))
            .text("File or named pipe where to write the submissions csv to. Default value: 'submissions.csv'"),

          opt[File]('z', "comments-out")
            .valueName("<file>")
            .action((x, c) => c.copy(submissionsOutFile = x))
            .text("File or named pipe where to write the comments csv to. Default value: 'commentsdd.csv'"),
        ),

      cmd("statistics")
        .action((_, c) => c.copy(programMode = ProgramMode.Statistics))
        .text("Runs the program in statistics mode.")
        .children(
          cmd("usercount")
            .action((_, c) => c.copy(experiment = Experiment.UserCount))
            .text("Experiment to count the users that commented or posted.")
        ),
    )
  }

  // OParser.parse returns Option[Config]
  OParser.parse(parser1, args, Config()) match {
    case Some(config) => {

      println(f"Program mode: ${config.programMode}")

      config.programMode match {

        case ProgramMode.PassTrough => {
          if(!config.provideSubmissionsStream && !config.provideCommentsStream){
            println("Neither '--submissions' nor '--comments' option enabled. No output will be generated.")
            exit(1)
          }

          // ToDo: add passtrough
          println("Implement pass trough")
        }

        case ProgramMode.Statistics => {
          println(f"Experiment: ${config.experiment}")

          // Select experiment
          config.experiment match {
            case Experiment.UserCount => println("matched user count")
            case _ => {
              println("Experiment not implemented yet.")
              exit(1)
            }
          }
        }

        case _ => {
          println("No program mode specified. Run the program with --help to see available commands.")
          exit(1)
        }
      }
    }
    case _ =>
      // arguments are bad, error message will have been displayed
      print("failed")
      exit(1)
  }

  exit()
  /////////////////



  implicit val system = ActorSystem("ReadArchives")
  val fileOut = "/home/andreas/rpipe"
  val submissionsDirectory = Paths.get("./submissions");
  val numberOfThreads = 6;


  val startTime = System.nanoTime


  val sink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(Paths.get(fileOut))

  val filesSource: Source[Path, NotUsed] = Directory.ls(submissionsDirectory).filter(p => p.getFileName.toString.startsWith("RS_"))

  val fileSink = FileIO.toPath(Paths.get(fileOut))
  val countSink = Sink.fold[Int, ByteString](0)((acc, _) => acc + 1)

  val (eventualResult, countResult) = filesSource
    .flatMapMerge(numberOfThreads, file => {
      println(file)

      getCompressorInputStreamSource(file.toString)
        .via(Flows.ndJsonToSubmission).async
        .via(Flows.objectToCsv)
    })
    .alsoToMat(fileSink)(Keep.right)
    .toMat(countSink)(Keep.both)
    .run()

  println("rpipe is ready to be read.")

  implicit val ec = system.dispatcher
  eventualResult.onComplete {
    case util.Success(_) => {
      println("Pipeline finished successfully.")
      completeAndTerminate()
    }
    case util.Failure(e) => {
      println(s"Pipeline failed with $e")
      completeAndTerminate()
    }
  }

  val count = Await.result(countResult, 365.days)
  println(f"Count: $count")



  def completeAndTerminate() ={
    val duration = (System.nanoTime - startTime) / 1e9d
    println(duration)

    system.terminate()
    val durationTerminated = (System.nanoTime - startTime) / 1e9d
    print("terminated after: ")
    println(durationTerminated)
  }

  @throws[FileNotFoundException]
  @throws[CompressorException]
  def getCompressorInputStreamSource(fileName: String): Source[ByteString, Future[IOResult]] = {
    val fileInputStream = new FileInputStream(new File(fileName))
    val bufferedInputStream = new BufferedInputStream(fileInputStream)
    val compressionName = CompressorStreamFactory.detect(bufferedInputStream)
    val compressorInputStream = new CompressorStreamFactory()
      .createCompressorInputStream(compressionName, bufferedInputStream, true)
    StreamConverters.fromInputStream(() => compressorInputStream)
  }
}
