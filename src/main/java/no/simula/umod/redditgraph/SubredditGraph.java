package no.simula.umod.redditgraph;


import com.opencsv.bean.CsvToBeanBuilder;
import org.apache.commons.compress.compressors.CompressorException;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultUndirectedWeightedGraph;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import static no.simula.umod.redditgraph.ConsoleUtils.log;
import static no.simula.umod.redditgraph.ConsoleUtils.logDuration;

class SubredditGraph {

    private final Graph<SrVertex, Edge> g = new DefaultUndirectedWeightedGraph<>(Edge.class);
    // subreddit - Subreddit Vertex
    private final Map<String, SrVertex> vertexMap = new HashMap<>(10000);

    public void createGraphFromUserSubredditCsv(List<File> inputFile) throws IOException, CompressorException {
        long startTime = System.nanoTime();
        log("Start read csv to hashmap.");
        HashMap<String, HashSet<String>> users = new HashMap<>(10000000);

        // read the subreddit,user files
        for (File file : inputFile) {
            final var subredditUser = FileUtils.readCsv(file);

            for (final var entry : subredditUser) {

                // Create subreddit list per user
                users.putIfAbsent(entry[1], new HashSet<>());
                users.get(entry[1]).add(entry[0]);

                // Create a unique vertex per subreddit
                if (vertexMap.containsKey(entry[0]))
                    continue;

                final var vertex = new SrVertex(entry[0], g);
                vertexMap.put(entry[0], vertex);
                g.addVertex(vertex);
            }
        }


        logDuration("Finished adding vertices and create user->sr list", startTime);
        startTime = System.nanoTime();

        // Add all the sr edges per user -> subreddit x subreddit
        users.forEach((user, subreddits) -> {
            // list of subreddits for user
            final String[] arr = new String[subreddits.size()];
            subreddits.toArray(arr);
            subreddits.clear();


            for (int i = 0; i < arr.length; i++) {
                for (int j = i + 1; j < arr.length; j++) {
                    // Undirected unique edge per user
                    final var src = vertexMap.get(arr[i]);
                    final var tar = vertexMap.get(arr[j]);
                    final var edge = g.getEdge(src, tar);
                    if (edge != null) {
                        edge.incrementNumberOfUsersInBothSubreddits();
                    } else {
                        g.addEdge(src, tar, new Edge(g));
                    }
                }
            }
        });

        logDuration("Finished building the graph", startTime);
        startTime = System.nanoTime();
        users = null;

        // Calculate vertex scores that are not dependent on edge scores
        vertexMap.values().parallelStream().forEach(SrVertex::calculateIndependentScores);
        logDuration("Finished calculating the independent vertex scores", startTime);
        startTime = System.nanoTime();

        // Calculate edge scores that depend on vertex scores
        g.edgeSet().parallelStream().forEach(Edge::calculateScoresDependentOnVertex);
        logDuration("Finished calculating the edge scores that depend on vertex scores", startTime);
        startTime = System.nanoTime();

        // Calculate vertex scores that are dependent on edge scores (e.g. weight)
        vertexMap.values().parallelStream().forEach(SrVertex::calculateScoresDependentOnEdgeScores);
        logDuration("Finished calculating the edge dependent vertex scores", startTime);

        log("# Vertices " + g.vertexSet().size());
        log("# Edges    " + g.edgeSet().size());
    }

    public void loadGraphFromVertexEdgeList(List<File> inputFile) throws IOException, CompressorException {
        final var vertexFile = inputFile.stream().filter(f -> f.getName().contains("vertex")).findFirst().get();
        final var edgeFile = inputFile.stream().filter(f -> f.getName().contains("edge")).findFirst().get();

        // Load vertices
        {
            final var vertexReader = FileUtils.getFileReaderBasedOnType(vertexFile);
            final Stream<SrVertex> vtxStream = new CsvToBeanBuilder<SrVertex>(vertexReader)
                    .withType(SrVertex.class)
                    .withOrderedResults(false)
                    .build()
                    .stream();

            vtxStream.forEach(vertex -> {
                vertex.setGraph(g);

                g.addVertex(vertex);
                vertexMap.put(vertex.subreddit, vertex);
            });
        }

        // Load edges
        final var edgeReader = FileUtils.getFileReaderBasedOnType(edgeFile);
        final Stream<Edge> edgeStream = new CsvToBeanBuilder<Edge>(edgeReader)
                .withType(Edge.class)
                .withOrderedResults(false)
                .build()
                .stream();

        edgeStream.forEach(edge -> {
            edge.setGraph(g);

            final var src = vertexMap.get(edge.sourceName);
            final var tar = vertexMap.get(edge.targetName);
            g.addEdge(src, tar, edge);
        });

    }

    public CompletionStage<Void> exportVertexList(File outFile) {
        final var headers = new String[]{
                "i",
                "Ui",
                "degree",
                "weighted-degree",
                "degree-degree",
                "weighted-degree-degree",
                "local-clustering-coefficient"
        };

        return FileUtils.exportCsv(vertexMap.values(), outFile, headers);
    }

    public CompletionStage<Void> exportEdgeList(File outFile) {
        final var headers = new String[]{
                "i",
                "j",
                "U_ij",
                "degree_i",
                "degree_j",
                "weighted-degree_i",
                "weighted-degree_j",
                "avg-weighted-edge-weight_i",
                "avg-weighted-edge-weight_j",
                "W_ij"
        };

        return FileUtils.exportCsv(g.edgeSet(), outFile, headers);
    }

    public CompletionStage<Void> exportDot(File outFile) {

        return CompletableFuture.runAsync(() -> {
            final DOTExporter<SrVertex, Edge> exporter =
                    new DOTExporter<>(v -> '"' + v.toString().replace(".", "_") + '"');

            exporter.setVertexAttributeProvider(vertex -> {
                final Map<String, Attribute> map = new LinkedHashMap<>();
                map.put("degree", DefaultAttribute.createAttribute(g.degreeOf(vertex)));
                map.put("weightedDegree", DefaultAttribute.createAttribute(vertex.sumOfEdgeWeightsConnectedToVertex));
                map.put("degreeDegree", DefaultAttribute.createAttribute(vertex.getDegreeDegree()));
                map.put("weightedDegreeDegree", DefaultAttribute.createAttribute(vertex.getWeightedDegreeDegree()));
                map.put("localClusteringCoefficient", DefaultAttribute.createAttribute(vertex.getLocalClusteringCoefficient()));

                return map;
            });

            exporter.setEdgeAttributeProvider(edge -> {
                final Map<String, Attribute> map = new LinkedHashMap<>();
                map.put("weight", DefaultAttribute.createAttribute(edge.getWeight()));

                return map;
            });


            try {
                final var fileWriter = new FileWriter(outFile);
                final var bufferedWriter = new BufferedWriter(fileWriter);
                exporter.exportGraph(g, bufferedWriter);
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}

