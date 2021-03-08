package no.simula.umod.redditgraph;


import org.apache.commons.compress.compressors.CompressorException;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultUndirectedWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
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

import static no.simula.umod.redditgraph.ConsoleUtils.log;
import static no.simula.umod.redditgraph.ConsoleUtils.logDuration;

class SubRedditGraph {

    private final Graph<SrVertex, Edge> g = new DefaultUndirectedWeightedGraph<>(Edge.class);
    // subreddit - Subreddit Vertex
    private final Map<String, SrVertex> vertexMap =  new HashMap<>(10000);

        public void createCountListFromCsv(List<File> inputFile) throws IOException, CompressorException {
        long startTime = System.nanoTime();
        log("Start read csv to hashmap.");
        final HashMap<String, HashSet<String>> users = new HashMap<>(10000000);

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

                final var vertex = new SrVertex(entry[0]);
                vertexMap.put(entry[0],vertex);
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


    public CompletionStage<Void> exportVertexList(File outFile) {
        return FileUtils.exportCsv(vertexMap.values(), outFile);
    }

    public CompletionStage<Void> exportEdgeList(File outFile) {
        return FileUtils.exportCsv(g.edgeSet(), outFile);
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
                final var fileWriter = new FileWriter(outFile);
                final var bufferedWriter = new BufferedWriter(fileWriter);
                exporter.exportGraph(g, bufferedWriter);
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }


    class SrVertex implements ToCsv{

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

        public String[] toCsvLine() {
            return new String[]{
                    subreddit,
                    String.valueOf(sumOfEdgeWeightsConnectedToVertex), // Ui
                    String.valueOf(getDegree()), // degree(i)
                    String.valueOf(getWeightedDegree()), // weightedDegree (i)
                    String.valueOf(degreeDegree), // degreeDegree(i)
                    String.valueOf(weightedDegreeDegree), // weightedDegreeDegree
                    String.valueOf(localClusteringCoefficient) // local clustering coefficient
            };
        }
    }

    class Edge extends DefaultWeightedEdge implements ToCsv {

        private String sourceName; // i
        private String targetName; // j
        private int numberOfUsersInThoseSubreddits = 1; // Uij

        private int sourceDegree; // degree(i)
        private int targetDegree; // degree(j)

        private double weightedTargetDegree;            // weightedDegree(i)
        private double weightedSourceDegree;            // weightedDegree(j)

        private double avgWeightedTargetEdgeWeight;
        private double avgWeightedSourceEdgeWeight;

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
            return sourceDegree;
        }

        /**
         * degree j
         */
        public int getTargetDegree() {
            return targetDegree;
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

        public double getAvgWeightedTargetEdgeWeight() {
            return avgWeightedTargetEdgeWeight;
        }

        public void setAvgWeightedTargetEdgeWeight(double avgWeightedTargetEdgeWeight) {
            this.avgWeightedTargetEdgeWeight = avgWeightedTargetEdgeWeight;
        }

        public double getAvgWeightedSourceEdgeWeight() {
            return avgWeightedSourceEdgeWeight;
        }

        public void setAvgWeightedSourceEdgeWeight(double avgWeightedSourceEdgeWeight) {
            this.avgWeightedSourceEdgeWeight = avgWeightedSourceEdgeWeight;
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

            this.sourceName = source.subreddit;
            this.targetName = target.subreddit;
            this.sourceDegree = g.degreeOf(source);
            this.targetDegree = g.degreeOf(target);

            this.weightedSourceDegree =
                    source.getSumOfEdgeWeightsConnectedToVertex();

            this.weightedTargetDegree =
                    target.getSumOfEdgeWeightsConnectedToVertex();

            this.avgWeightedSourceEdgeWeight =
                    (source.getSumOfEdgeWeightsConnectedToVertex() / (double)sourceDegree);

            this.avgWeightedTargetEdgeWeight =
                    (target.getSumOfEdgeWeightsConnectedToVertex() / (double)targetDegree);

            calculateWeight(weightedSourceDegree, weightedTargetDegree);
        }

        /**
         * Wij
         */
        private void calculateWeight(final double avgi, final double avgj) {
            this.weight = (double)numberOfUsersInThoseSubreddits / ((avgi + avgj) / 2); // todo: (avgi + avgj) / 2 yes no???
        }

        public String[] toCsvLine() {

            return new String[] {
                    sourceName, // i
                    targetName, // j
                    String.valueOf(numberOfUsersInThoseSubreddits), // Uij
                    String.valueOf(sourceDegree), // degree(i)
                    String.valueOf(targetDegree), // degree(j)
                    String.valueOf(weightedSourceDegree), // weightedDegree(i)
                    String.valueOf(weightedTargetDegree), // weightedDegree(j)
                    String.valueOf(avgWeightedSourceEdgeWeight), // avgWeightedSourceEdgeWeight
                    String.valueOf(avgWeightedTargetEdgeWeight), // avgWeightedTargetEdgeWeight
                    String.valueOf(weight) // Wij
            };
        }
    }
}

