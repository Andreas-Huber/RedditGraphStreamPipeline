package no.simula.umod.redditgraph;

import org.apache.commons.compress.compressors.CompressorException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import static no.simula.umod.redditgraph.ConsoleUtils.logDuration;


enum ProgramMode {
    CreateFromUserSubredditCsvAndExport,
    LoadFromVertexEdgeList
}

@SuppressWarnings("unused")
@Command(name = "rgraph", mixinStandardHelpOptions = true, version = "not versioned / latest build from master",
        description = "RedditGraph for graph generation and experiments.")
class Main implements Callable<Integer> {

    @Parameters(description = "Files to lad the graph from.")
    private List<File> files;

    @Option(names= {"--mode"}, description = "Modes: ${COMPLETION-CANDIDATES}")
    private ProgramMode programMode;

    @Option(names= {"--out-edge-csv"}, description = "Output file for the edge list csv.")
    private File outEdgeCsv;

    @Option(names= {"--out-vertex-csv"}, description = "Output file for the vertex list csv.")
    private File outVertexCsv;

    @Option(names= {"--out-dot"}, description = "Output file for the graph dot.")
    private File outDot;

    @Override
    public Integer call() throws Exception {

        if(programMode == ProgramMode.CreateFromUserSubredditCsvAndExport)
            CreateFromUserSubredditCsvAndExport();
        else if (programMode == ProgramMode.LoadFromVertexEdgeList)
            LoadFromVertexEdgeList();
        return 0;
    }

    private void LoadFromVertexEdgeList() throws IOException, CompressorException {
        final var subredditGraph = new SubredditGraph();

        // Reimport graph
        subredditGraph.loadGraphFromVertexEdgeList(files);

        // Parallel export
        final var startTime = System.nanoTime();

        var dotFuture = subredditGraph.exportDot(outDot).thenRunAsync(() ->
                logDuration("Exported dot", startTime)
        ).toCompletableFuture();

        dotFuture.join();
    }

    private void CreateFromUserSubredditCsvAndExport() throws IOException, CompressorException {
        final var subredditGraph = new SubredditGraph();

        // Import and create
        subredditGraph.createGraphFromUserSubredditCsv(files);

        // Parallel export
        final var startTime = System.nanoTime();

        var dotFuture = subredditGraph.exportDot(outDot).thenRunAsync(() ->
                logDuration("Exported dot", startTime)
        ).toCompletableFuture();

        var edgeCsvFuture = subredditGraph.exportEdgeList(outEdgeCsv).thenRunAsync(() ->
                logDuration("Exported edge list", startTime)
        ).toCompletableFuture();

        var vertexCsvFuture = subredditGraph.exportVertexList(outVertexCsv).thenRunAsync(() ->
                logDuration("Exported vertex list", startTime)
        ).toCompletableFuture();

        dotFuture.join();
        edgeCsvFuture.join();
        vertexCsvFuture.join();
    }

    /**
     * Parses the command line arguments an runs the callable
     * @param args command line arguments
     */
    public static void main(String... args) {
        long startTime = System.nanoTime();
        int exitCode = new CommandLine(new Main()).execute(args);
        logDuration("Finished program run", startTime);
        System.exit(exitCode);
    }
}