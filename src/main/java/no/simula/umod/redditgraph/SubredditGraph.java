package no.simula.umod.redditgraph;


import com.opencsv.CSVWriter;
import org.apache.commons.compress.compressors.CompressorException;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultUndirectedWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import static no.simula.umod.redditgraph.ConsoleUtils.logDuration;

class SubRedditGraph {

    final Graph<String, Edge> g = new DefaultUndirectedWeightedGraph<>(Edge.class);

    public void createCountListFromCsv(File inputFile) throws IOException, CompressorException {
        long startTime = System.nanoTime();
        final var subredditUser = FileUtils.readAll(inputFile);
        final HashMap<String, HashSet<String>> users = new HashMap<>();

        for (final var entry : subredditUser) {
            // Create all vertices (duplicates handled by jgrapht)
            g.addVertex(entry[0]);

            // Create subreddit list per user
            users.putIfAbsent(entry[1], new HashSet<>());
            users.get(entry[1]).add(entry[0]);
        }

        logDuration("Finished adding vertices and create user->sr list", startTime);
        startTime = System.nanoTime();

        // Add all the sr edges per user -> subreddit x subreddit
        users.forEach((user, subreddits) -> {
            final String[] arr = new String[subreddits.size()];
            subreddits.toArray(arr);
            subreddits.clear(); //todo: Does that make sense to "free up some ram"

            for (int i = 0; i < arr.length; i++) {
                for (int j = i + 1; j < arr.length; j++) {
                    // Undirected unique edge per user
                    // Duplicated between the users possible, but jgrapht handles that for strings

                    final var edge = g.getEdge(arr[i], arr[j]);
                    if (edge != null) {
                        edge.incrementWeight();
                    } else {
                        g.addEdge(arr[i], arr[j], new Edge());
                    }
                }
            }
        });

        logDuration("# Vertices " + g.vertexSet().size(), startTime);
        logDuration("# Edges    " + g.edgeSet().size(), startTime);
    }

    public void createSample() {


        var a = "Politics";
        var b = "AskReddit";
        var c = "Gaming";

        g.addVertex(a);
        g.addVertex(b);
        g.addVertex(c);
        g.addVertex(c);

        g.addEdge(a, c);
        g.addEdge(a, c);
        g.addEdge(a, c);
        g.addEdge(a, c);
        g.addEdge(b, c);
        g.addEdge(a, a);
    }

    public void exportEdgeList(File outFile) throws IOException {
        final long startTime = System.nanoTime();
        final Writer writer = new FileWriter(outFile);
        final var csv = new CSVWriter(writer);

        g.edgeSet().forEach(edge -> csv.writeNext(edge.getEdgeCsvLine()));

        csv.flush();
        csv.close();
        writer.close();
        logDuration("Exported edge list", startTime);
    }

    public void exportDot(File outFile) throws IOException {
        final long startTime = System.nanoTime();
        final DOTExporter<String, Edge> exporter =
                new DOTExporter<>(v -> '"' + v.replace(".", "_") + '"');

        exporter.setEdgeAttributeProvider((edge) -> {
            final Map<String, Attribute> map = new LinkedHashMap<>();
            map.put("weight", DefaultAttribute.createAttribute(edge.numberOfUsersInThoseSubreddits));

            return map;
        });

        final Writer writer = new FileWriter(outFile);

        exporter.exportGraph(g, writer);
        writer.close();
        logDuration("Exported dot", startTime);
    }

    class Edge extends DefaultWeightedEdge {

        private int numberOfUsersInThoseSubreddits = 1;

        @Override
        public double getWeight() {
            return numberOfUsersInThoseSubreddits;
        }

        public int getWeightInt() {
            return numberOfUsersInThoseSubreddits;
        }

        public String getSrc() {
            return this.getSource().toString();
        }

        public String getTar() {
            return this.getTarget().toString();
        }

        public int getSourceDegree() {
            return g.degreeOf((String) getSource());
        }

        public int getTargetDegree() {
            return g.degreeOf((String) getSource());
        }

        public int getWeightedSourceDegree() {
            return getSumOfEdgeWeightsConnectedToVertex(getSrc());
        }

        public int getWeightedTargetDegree() {
            return getSumOfEdgeWeightsConnectedToVertex(getTar());
        }

        /**
         * Sum up all the edge weights connected a vertex
         *
         * @param vertex to get the edge weights from
         * @return
         */
        private int getSumOfEdgeWeightsConnectedToVertex(String vertex) {
            int sum = 0;
            for (Edge edge : g.edgesOf(vertex)) {
                //todo: Daniel, sum of weights or sum of degrees?
                sum += edge.getWeightInt();
            }
            return sum;
        }

        public void incrementWeight() {
            numberOfUsersInThoseSubreddits++;
        }

        public String[] getEdgeCsvLine() {
            return new String[]{
                    getSrc(),
                    getTar(),
                    String.valueOf(numberOfUsersInThoseSubreddits),
                    String.valueOf(getSourceDegree()),
                    String.valueOf(getTargetDegree()),
                    String.valueOf(getWeightedSourceDegree()),
                    String.valueOf(getWeightedTargetDegree()),
            };
        }
    }
}

