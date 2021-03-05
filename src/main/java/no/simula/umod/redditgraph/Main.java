package no.simula.umod.redditgraph;

import akka.actor.ActorSystem;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.NotImplementedException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.util.concurrent.Callable;

import static no.simula.umod.redditgraph.ConsoleUtils.*;

enum ProgramMode {
    UnweightedGraph
}

@Command(name = "rgraph", mixinStandardHelpOptions = true, version = "not versioned / latest build from master",
        description = "RedditGraph for graph generation and experiments.")
class Main implements Callable<Integer> {

    private final ActorSystem actorSystem;

    public Main() {
        final var config = ConfigFactory.parseString("""
                      akka.actor.default-blocking-io-dispatcher.thread-pool-executor.fixed-pool-size = "128"
                      akka.actor.default-dispatcher.fork-join-executor.parallelism-max = "1024"
                """).withFallback(ConfigFactory.load());
        actorSystem = ActorSystem.create("Graph", config);
    }

    @Parameters(index = "0", description = "Valid values: ${COMPLETION-CANDIDATES}")
    private ProgramMode mode;

    @Parameters(index = "1", description = "File to lad the graph from.")
    private File file;

    @Option(names= {"--out-edge-csv"}, description = "Output file for the edge list csv.")
    private File outEdgeCsv;

    @Option(names= {"--out-dot"}, description = "Output file for the graph dot.")
    private File outDot;

    @Override
    public Integer call() throws Exception {
        if(mode == ProgramMode.UnweightedGraph){
            log(Thread.currentThread());
            final var subredditGraph = new SubRedditGraph(actorSystem);

            // Import and create
            subredditGraph.createCountListFromCsv(file);

            // Parallel export
            var dotFuture = subredditGraph.exportDot(outDot);

            final var startTime = System.nanoTime();
            var csvFuture = subredditGraph.exportEdgeList(outEdgeCsv).thenRunAsync(() ->
                    logDuration("Exported edge list", startTime)
            ).toCompletableFuture();


            dotFuture.join();
            csvFuture.join();
        }
        else {
            throw new NotImplementedException("Mode not implemented");
        }

        return 0;
    }

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        long startTime = System.nanoTime();
        int exitCode = new CommandLine(new Main()).execute(args);
        logDuration("Finished program run", startTime);
        System.exit(exitCode);
    }
}