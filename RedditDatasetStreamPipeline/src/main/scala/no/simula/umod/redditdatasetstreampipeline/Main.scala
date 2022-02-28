package no.simula.umod.redditdatasetstreampipeline

import java.io.File
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import no.simula.umod.redditdatasetstreampipeline.ConsoleTools.log
import scopt.OParser

import java.util.Calendar
import scala.sys.exit

object Main extends App {

  val startTime = System.nanoTime

  val config = ConfigFactory.parseString(s"""
      akka.actor.default-blocking-io-dispatcher.thread-pool-executor.fixed-pool-size = "128"
      akka.actor.default-dispatcher.fork-join-executor.parallelism-max = "1024"
    """).withFallback(ConfigFactory.load())

  implicit val system: ActorSystem = ActorSystem("ReadArchives", config)

  val builder = OParser.builder[Config]
  val parser1 = {
    import builder._
    OParser.sequence(
      programName("redditdatasetstreampipeline"),
      head("Reddit Dataset Stream Pipeline", "0.1"),

      opt[File]('i', "dataset-dir")
        .valueName("<dir>")
        .action((x, c) => c.copy(datasetDirectory = x))
        .text("Dataset directory that contains the submissions and comments folder. Default value: 'redditdataset'."),

      opt[Int]('p', "parallel")
        .valueName("<n>")
        .action((x, c) => c.copy(numberOfConcurrentFiles = x))
        .validate(x =>
          if (x > 0) success
          else failure("Value <number> must be > 0"))
        .text("Number of how many files should be read concurrently."),

      opt[String]("filter")
        .valueName("<filter>")
        .action((x, c) => c.copy(fileNameContainsFilter = x))
        .text("File name contains filter."),

      opt[String]("exclude")
        .valueName("<exclude>")
        .action((x, c) => c.copy(fileNameNotContainsFilter = x))
        .text("File name not contains filter."),

      opt[Unit]("compress")
        .action((_, c) => c.copy(compressOutput = true))
        .text("If enabled, the output will be compressed using ZSTD."),

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

          opt[File]("submission-out")
            .valueName("<file>")
            .action((x, c) => c.copy(submissionsOutFile = x))
            .text("File or named pipe where to write the submissions csv to. Default value: 'submissions.csv'"),

          opt[File]("comment-out")
            .valueName("<file>")
            .action((x, c) => c.copy(commentsOutFile = x))
            .text("File or named pipe where to write the comments csv to. Default value: 'comments.csv'"),

          opt[File]("author-out")
            .valueName("<file>")
            .action((x, c) => c.copy(authorsOutFile = x))
            .text("File or named pipe where to write the authors csv to. Default value: 'authors.csv'"),

          opt[File]("filter-by-sr")
            .valueName("<file>")
            .action((x, c) => c.copy(filterBySubreddits = x))
            .text("Filter by a newline separated list of subreddits."),

          opt[Unit]("keep-original-json")
            .action((_, c) => c.copy(keepOriginalJson = true))
            .text("If enabled, the program writes the original json to the stream."),

          opt[Unit]("only-user-in-sr")
            .action((_, c) => c.copy(onlyUserInSubreddit = true))
            .text("If enabled, the program writes only a subreddit,user csv."),
        ),

      cmd("statistics")
        .action((_, c) => c.copy(programMode = ProgramMode.Statistics))
        .text("Runs the program in statistics mode.")

        .children(
          cmd("UserContributionsInSubreddits")
            .action((_, c) => c.copy(experiment = Experiment.UserContributionsInSubreddits))
            .text("Experiment to count the number of contributions users made in subreddits. A contribution is a post or comment."),

          cmd("UsersInSubreddits")
            .action((_, c) => c.copy(experiment = Experiment.UsersInSubreddits))
            .text("Experiment to count the users that made at least one contribution in a subreddit. A contribution is a post or comment."),

          opt[String]("experiment-suffix")
            .valueName("<suffix>")
            .action((x, c) => c.copy(experimentSuffix = x))
            .text("Experiment out file suffix, e.g. to append a distinct value like a year."),

          opt[File]("statistics-out")
            .valueName("<dir>")
            .action((x, c) => c.copy(statisticsOutDir = x))
            .text("Directory where the results of the experiments shall be written to. Default value: '~'"),
        ),
    )
  }

  val mb = 1024*1024
  val runtime = Runtime.getRuntime
  println("Memory Used  MB:     " + (runtime.totalMemory - runtime.freeMemory) / mb)
  println("Memory Total MB:     " + runtime.totalMemory / mb)
  println("Memory Max   MB:     " + runtime.maxMemory / mb)

  // Parse and chose actions based on the selected options
  OParser.parse(parser1, args, Config()) match {
    case Some(config) =>

      println(f"Program mode:       ${config.programMode}")
      println(s"Started at:         ${Calendar.getInstance().getTime()}")
      println(f"Dataset directory:  ${config.datasetDirectory}")
      println(f"Filter:             ${config.fileNameContainsFilter}")
      println(f"Exclude:            ${config.fileNameNotContainsFilter}")
      println(f"Subreddit Filter:   ${config.filterBySubreddits}")

      config.programMode match {

        case ProgramMode.PassTrough =>
          if(!config.provideSubmissionsStream && !config.provideCommentsStream && !config.provideAuthorsStream){
            println("Error: No stream enabled. No output will be generated.")
            println("Add the '--submissions', '--comments' or '--authors' option.")
            exit(1)
          }

          if(config.keepOriginalJson && config.onlyUserInSubreddit) {
            println("Error: Decide if you want json or only users and subreddits as a csv.")
            println("Remove either the '--keep-original-json' or the '--only-user-in-sr' option.")
            exit(1)
          }

          val passTrough = new PassTrough(system, config)
          passTrough.runPassTrough()

        case ProgramMode.Statistics =>

          println(f"Experiment:         ${config.experiment}")
          val statistics = new Statistics(system, config)

          // Select experiment
          config.experiment match {
            case Experiment.UserContributionsInSubreddits => statistics.runUserContributionsInSubreddits()
            case Experiment.UsersInSubreddits => statistics.runUsersInSubreddits()
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

  def completeAndTerminate(): Unit = {
    val duration = (System.nanoTime - startTime) / 1e9d
    log(f"Finished after: $duration seconds")
    system.terminate()
  }
}
