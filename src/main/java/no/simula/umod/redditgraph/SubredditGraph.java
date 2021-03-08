package no.simula.umod.redditgraph;


import akka.actor.ActorSystem;
import akka.stream.IOResult;
import akka.stream.alpakka.csv.javadsl.CsvFormatting;
import akka.stream.alpakka.csv.javadsl.CsvQuotingStyle;
import akka.stream.javadsl.Source;
import org.apache.commons.compress.compressors.CompressorException;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultUndirectedWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static no.simula.umod.redditgraph.ConsoleUtils.log;
import static no.simula.umod.redditgraph.ConsoleUtils.logDuration;

class SubRedditGraph {

    private final Graph<SrVertex, Edge> g = new DefaultUndirectedWeightedGraph<>(Edge.class);
    // subreddit - Subreddit Vertex
    private final Map<String, SrVertex> vertexMap =  new HashMap<>(10000);

    private final ActorSystem actorSystem;

    public SubRedditGraph(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    public void createCountListFromCsv(File inputFile) throws IOException, CompressorException {
        long startTime = System.nanoTime();
        log("Start read csv to hashmap.");
        final var subredditUser = FileUtils.readCsv(inputFile);
        final HashMap<String, HashSet<String>> users = new HashMap<>(10000000);

//        var source = Flows.getFileSource(inputFile);
//        var c = source
//                .via(CsvParsing.lineScanner())
//                .via(CsvToMap.toMapAsStrings(StandardCharsets.UTF_8))
//                .map(a -> {
//                    final var arr = new String[2];
//                    a.values().toArray(arr);
//                    return arr;
//                })
//                .runForeach(entry -> {
//                    // Create all vertices (duplicates handled by jgrapht)
//                    g.addVertex(new SrVertex(entry[0]));
//
//                    // Create subreddit list per user
//                    users.putIfAbsent(entry[1], new HashSet<>());
//                    users.get(entry[1]).add(entry[0]);
//                }, actorSystem);

//        c.toCompletableFuture().join();

        // the subreddit,user file
        for (final var entry : subredditUser) {

            // Create subreddit list per user
            users.putIfAbsent(entry[1], new HashSet<>());
            users.get(entry[1]).add(entry[0]);

            // Create a unique vertex per subreddit
            if (vertexMap.containsKey(entry[0]))
                continue;

            final var vertex = new SrVertex(entry[0]);
            vertexMap.put(entry[0],vertex);
            g.addVertex(vertex);
        }

        logDuration("Finished adding vertices and create user->sr list", startTime);
        startTime = System.nanoTime();

        // Add all the sr edges per user -> subreddit x subreddit
        users.forEach((user, subreddits) -> {
            // list of subreddits for user
            final String[] arr = new String[subreddits.size()];
            subreddits.toArray(arr);
            subreddits.clear(); //todo: Does that make sense to "free up some ram"


            for (int i = 0; i < arr.length; i++) {
                for (int j = i + 1; j < arr.length; j++) {
                    // Undirected unique edge per user
                    final var src = vertexMap.get(arr[i]);
                    final var tar = vertexMap.get(arr[j]);
                    final var edge = g.getEdge(src, tar);
                    if (edge != null) {
                        edge.incrementNumberOfUsersInBothSubreddits();
                    } else {
                        g.addEdge(src, tar, new Edge());
                    }
                }
            }
        });

        logDuration("Finished building the graph", startTime);
        startTime = System.nanoTime();

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


    public CompletionStage<IOResult> exportVertexList(File outFile) throws IOException, CompressorException {

        final var fileSink = Flows.getFileSink(outFile);
        final var csvFlow = CsvFormatting.format(CsvFormatting.COMMA,
                CsvFormatting.DOUBLE_QUOTE,
                CsvFormatting.BACKSLASH,
                "\n",
                CsvQuotingStyle.REQUIRED,
                StandardCharsets.UTF_8,
                Optional.empty());
        return Source.from(vertexMap.values())
                .map(SrVertex::getVertexCsvLine)
                .via(csvFlow)
                .async()
                .runWith(fileSink, actorSystem);
    }

    public CompletionStage<Void> exportEdgeList(File outFile) throws IOException, CompressorException {

//        final var fileSink = Flows.getFileSink(outFile);
//        final var csvFlow = CsvFormatting.format(CsvFormatting.COMMA,
//                CsvFormatting.DOUBLE_QUOTE,
//                CsvFormatting.BACKSLASH,
//                "\n",
//                CsvQuotingStyle.REQUIRED,
//                StandardCharsets.UTF_8,
//                Optional.empty());
//        return Source.from(g.edgeSet())
//                .map(Edge::getEdgeCsvLine)
//                .via(csvFlow)
//                .async()
//                .runWith(fileSink, actorSystem);

        return CompletableFuture.runAsync(() -> {
            try {
                final var writer = FileUtils.createWriter(outFile);

                for (Edge edge : g.edgeSet()) {
                    writer.write(String.join(",", edge.getEdgeCsvLine()));
                }
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public CompletionStage<Void> exportDot(File outFile) {

        return CompletableFuture.runAsync(() -> {
            final DOTExporter<SrVertex, Edge> exporter =
                    new DOTExporter<>(v -> '"' + v.toString().replace(".", "_") + '"');

            exporter.setVertexAttributeProvider(vertex -> {
                final Map<String, Attribute> map = new LinkedHashMap<>();
                map.put("degree", DefaultAttribute.createAttribute(g.degreeOf(vertex)));
                map.put("weightedDegree", DefaultAttribute.createAttribute(vertex.getSumOfEdgeWeightsConnectedToVertex()));
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
                final Writer writer = new FileWriter(outFile);
                exporter.exportGraph(g, writer);
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        });
    }


    //todo: export vertex degree, and weighted degree file. plot distribution.
    class SrVertex{

        private final String subreddit;
        private int sumOfEdgeWeightsConnectedToVertex = 0;
        private int degreeDegree;
        private double weightedDegreeDegree;
        private double localClusteringCoefficient;

        public SrVertex(String subreddit) {
            this.subreddit = subreddit;
        }

        public String getSubreddit() {
            return subreddit;
        }

        public int getDegreeDegree() {
            return degreeDegree;
        }

        public int getSumOfEdgeWeightsConnectedToVertex() {
            return this.sumOfEdgeWeightsConnectedToVertex;
        }

        public int getDegree(){
            return g.degreeOf(this);
        }

        public double getWeightedDegree(){
            return getSumOfEdgeWeightsConnectedToVertex() / (double)g.degreeOf(this);
        }

        public void setSumOfEdgeWeightsConnectedToVertex(int sumOfEdgeWeightsConnectedToVertex) {
            this.sumOfEdgeWeightsConnectedToVertex = sumOfEdgeWeightsConnectedToVertex;
        }

        public void setDegreeDegree(int degreeDegree) {
            this.degreeDegree = degreeDegree;
        }

        public double getWeightedDegreeDegree() {
            return weightedDegreeDegree;
        }

        public void setWeightedDegreeDegree(double weightedDegreeDegree) {
            this.weightedDegreeDegree = weightedDegreeDegree;
        }

        public double getLocalClusteringCoefficient() {
            return localClusteringCoefficient;
        }

        public void setLocalClusteringCoefficient(double localClusteringCoefficient) {
            this.localClusteringCoefficient = localClusteringCoefficient;
        }

        @Override
        public String toString() {
            return subreddit;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SrVertex that = (SrVertex) o;
            return subreddit.equals(that.subreddit);
        }

        @Override
        public int hashCode() {
            return subreddit.hashCode();
        }

        public void calculateIndependentScores(){
            this.sumOfEdgeWeightsConnectedToVertex = calculateSumOfEdgeWeightsConnectedToVertex();
        }

        public void calculateScoresDependentOnEdgeScores(){
            computeDegreeDegrees();
            this.localClusteringCoefficient =  computeLocalClusteringCoefficient();
        }

        /**
         * Sum up all the edge weights connected a vertex
         */
        private int calculateSumOfEdgeWeightsConnectedToVertex() {
            int sum = 0;

            for (Edge edge : g.edgesOf(this)) {
                sum += edge.getNumberOfUsersInBothSubreddits();
            }
            return sum;
        }

        private void computeDegreeDegrees() {
            int ddd = 0;
            double wdd = 0d;
            for (var vtx : Graphs.neighborListOf(g, this)) {
                for (var edge : g.edgesOf(vtx)) {
                    wdd += edge.getWeight();
                    ddd += 1;
                }
            }
            degreeDegree = ddd;
            weightedDegreeDegree = wdd;
        }

        private double computeLocalClusteringCoefficient() {
            final var neighbourhood = Graphs.neighborSetOf(g, this);
            final double k = neighbourhood.size();
            double numberTriplets = 0;
            for (var p : neighbourhood)
                for (var q : neighbourhood)
                    if (g.containsEdge(p, q))
                        numberTriplets++;
            if (k <= 1)
                return 0.0d;
            else
                return numberTriplets / (k * (k - 1));
        }

        public Collection<String> getVertexCsvLine() {
            return List.of(
                    subreddit,
                    String.valueOf(sumOfEdgeWeightsConnectedToVertex), // Ui
                    String.valueOf(getDegree()), // degree(i)
                    String.valueOf(getWeightedDegree()), // weightedDegree (i)
                    String.valueOf(degreeDegree), // degreeDegree(i)
                    String.valueOf(weightedDegreeDegree), // weightedDegreeDegree
                    String.valueOf(localClusteringCoefficient) // local clustering coefficient
            );
        }
    }

    class Edge extends DefaultWeightedEdge {

        private int numberOfUsersInThoseSubreddits = 1; // Uij
        private double weightedTargetDegree;            // weightedDegree(i)
        private double weightedSourceDegree;            // weightedDegree(j)
        private double weight;                          // Wij

        /**
         * i
         */
        public SrVertex getSrc() {
            return (SrVertex) this.getSource();
        }

        /**
         * j
         */
        public SrVertex getTar() {
            return (SrVertex) this.getTarget();
        }

        /**
         * Uij
         */
        public int getNumberOfUsersInBothSubreddits() {
            return numberOfUsersInThoseSubreddits;
        }
        /**
         * Uij
         */
        public void setNumberOfUsersInThoseSubreddits(int numberOfUsersInThoseSubreddits) {
            this.numberOfUsersInThoseSubreddits = numberOfUsersInThoseSubreddits;
        }

        /**
         * Increment Uij
         */
        public void incrementNumberOfUsersInBothSubreddits() {
            numberOfUsersInThoseSubreddits++;
        }

        /**
         * degree i
         */
        public int getSourceDegree() {
            return g.degreeOf((SrVertex) getSource());
        }

        /**
         * degree j
         */
        public int getTargetDegree() {
            return g.degreeOf((SrVertex) getSource());
        }

        /**
         * weightedDegree(i)
         */
        public double getWeightedTargetDegree() {
            return weightedTargetDegree;
        }

        /**
         * weightedDegree(i)
         */
        public void setWeightedTargetDegree(double weightedTargetDegree) {
            this.weightedTargetDegree = weightedTargetDegree;
        }

        /**
         * weightedDegree(j)
         */
        public double getWeightedSourceDegree() {
            return weightedSourceDegree;
        }

        /**
         * weightedDegree(j)
         */
        public void setWeightedSourceDegree(double weightedSourceDegree) {
            this.weightedSourceDegree = weightedSourceDegree;
        }


        /**
         * Wij, stateful and cached. Works only if the weight has been calculated or set before.
         */
        @Override
        public double getWeight() {
            return weight;
        }

        /**
         * Wij
         */
        public void setWeight(double weight){
            this.weight = weight;
        }

        /**
         * Calculates weights after the vertex scores have ben calculated
         */
        public void calculateScoresDependentOnVertex(){
            final SrVertex source = getSrc();
            final SrVertex target = getTar();
            final int sourceDegree = g.degreeOf(source);
            final int targetDegree = g.degreeOf(target);

            this.weightedSourceDegree = source.getSumOfEdgeWeightsConnectedToVertex() / (double)sourceDegree;
            this.weightedTargetDegree = target.getSumOfEdgeWeightsConnectedToVertex() / (double)targetDegree;
            calculateWeight(weightedSourceDegree, weightedTargetDegree);
        }

        /**
         * Wij
         */
        private void calculateWeight(final double avgi, final double avgj) {
            this.weight = (double)numberOfUsersInThoseSubreddits / ((avgi + avgj) / 2); // todo: (avgi + avgj) / 2 yes no???
        }

        public Collection<String> getEdgeCsvLine() {
            final SrVertex source = getSrc();
            final SrVertex target = getTar();
            final int sourceDegree = g.degreeOf(source);
            final int targetDegree = g.degreeOf(target);

            return List.of(
                    source.toString(), // i
                    target.toString(), // j
                    String.valueOf(numberOfUsersInThoseSubreddits), // Uij
                    String.valueOf(sourceDegree), // degree(i)
                    String.valueOf(targetDegree), // degree(j)
                    String.valueOf(weightedSourceDegree), // weightedDegree(i)
                    String.valueOf(weightedTargetDegree), // weightedDegree(j)
                    String.valueOf(weight) // Wij
            );
        }
    }
}

