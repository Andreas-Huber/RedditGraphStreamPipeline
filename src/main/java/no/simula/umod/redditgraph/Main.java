package no.simula.umod.redditgraph;

import org.apache.commons.lang3.NotImplementedException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.util.concurrent.Callable;

enum ProgramMode {
    UnweightedGraph
}

@Command(name = "rgraph", mixinStandardHelpOptions = true, version = "not versioned / latest build from master",
        description = "RedditGraph for graph generation and experiments.")
class Main implements Callable<Integer> {

    @Parameters(index = "0", description = "Valid values: ${COMPLETION-CANDIDATES}")
    private ProgramMode mode;

    @Parameters(index = "1", description = "File to lad the graph from.")
    private File file;

    @Option(names= {"-o", "--out-file"}, description = "Output file for the graph")
    private File outFile;

    @Override
    public Integer call() throws Exception {
        if(mode == ProgramMode.UnweightedGraph){
            final var subredditGraph = new SubRedditGraph();
            subredditGraph.createGraphFromCSV(file);
            subredditGraph.exportDot(outFile);
        }
        else {
            throw new NotImplementedException("Mode not implemented");
        }

        return 0;
    }

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}