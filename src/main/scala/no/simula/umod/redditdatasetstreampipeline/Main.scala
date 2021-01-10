package no.simula.umod.redditdatasetstreampipeline

import java.io.File
import akka.actor.ActorSystem
import no.simula.umod.redditdatasetstreampipeline.ConsoleTools.log
import scopt.OParser

import java.util.Calendar
import scala.sys.exit

object Main extends App {

  val startTime = System.nanoTime
  implicit val system: ActorSystem = ActorSystem("ReadArchives")

  val builder = OParser.builder[Config]
  val parser1 = {
    import builder._
    OParser.sequence(
      programName("redditdatasetstreampipeline"),
      head("Reddit Dataset Stream Pipeline", "0.1"),

      opt[File]('i', "dataset-dir")
        .valueName("<directory>")
        .action((x, c) => c.copy(datasetDirectory = x))
        .text("Dataset directory that contains the submissions and comments folder. Default value: 'redditdataset'."),

      opt[Int]('p', "concurrent-files")
        .valueName("<number 1 to n>")
        .action((x, c) => c.copy(numberOfConcurrentFiles = x))
        .validate(x =>
          if (x > 0) success
          else failure("Value <number> must be > 0"))
        .text("Number of how many files should be read concurrently."),

      opt[String]("filter")
        .valueName("<filter>")
        .action((x, c) => c.copy(fileNameContainsFilter = x))
        .text("File name contains filter."),

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

          opt[Unit]('a', "authors")
            .action((_, c) => c.copy(provideAuthorsStream = true))
            .text("Enables the Authors output stream."),

          opt[Unit]("count")
            .action((_, c) => c.copy(enableCount = true))
            .text("If enabled, the program counts the number of elements on the stream."),

          opt[File]("submissions-out")
            .valueName("<file>")
            .action((x, c) => c.copy(submissionsOutFile = x))
            .text("File or named pipe where to write the submissions csv to. Default value: 'submissions.csv'"),

          opt[File]("comments-out")
            .valueName("<file>")
            .action((x, c) => c.copy(commentsOutFile = x))
            .text("File or named pipe where to write the comments csv to. Default value: 'comments.csv'"),

          opt[File]("authors-out")
            .valueName("<file>")
            .action((x, c) => c.copy(authorsOutFile = x))
            .text("File or named pipe where to write the authors csv to. Default value: 'authors.csv'"),
        ),

      cmd("statistics")
        .action((_, c) => c.copy(programMode = ProgramMode.Statistics))
        .text("Runs the program in statistics mode.")
        .children(
          cmd("UserContributionsInSubreddits")
            .action((_, c) => c.copy(experiment = Experiment.UserContributionsInSubreddits))
            .text("Experiment to count the contributions users made in subreddits. A contribution is a post or comment.")
        ),
    )
  }

  // Parse and chose actions based on the selected options
  OParser.parse(parser1, args, Config()) match {
    case Some(config) =>

      println(f"Program mode:       ${config.programMode}")
      println(s"Started at:         ${Calendar.getInstance().getTime()}")
      println(f"Dataset directory:  ${config.datasetDirectory}")

      config.programMode match {

        case ProgramMode.PassTrough =>
          if(!config.provideSubmissionsStream && !config.provideCommentsStream && !config.provideAuthorsStream){
            println("Error: No stream enabled. No output will be generated.")
            println("Add the '--submissions', '--comments' or '--authors' option.")
            exit(1)
          }

          val passTrough = new PassTrough(system, config)
          passTrough.runPassTrough()

        case ProgramMode.Statistics =>

          println(f"Experiment:         ${config.experiment}")
          val statistics = new Statistics(system, config)

          // Select experiment
          config.experiment match {
            case Experiment.UserContributionsInSubreddits => statistics.runUserContributionsInSubreddits(config.experiment)
            case _ =>
              println("Error: Experiment not implemented yet.")
              exit(1)
          }

        case _ =>
          println("Error: No program mode specified. Run the program with --help to see available commands.")
          exit(1)
      }
    case _ =>
      // arguments are bad, error message will have been displayed
      print("failed")
      exit(1)
  }

  // Stop actor system
  completeAndTerminate()

  def completeAndTerminate(): Unit ={
    val duration = (System.nanoTime - startTime) / 1e9d
    log(f"Finished after: $duration seconds")
    system.terminate()
  }
}
