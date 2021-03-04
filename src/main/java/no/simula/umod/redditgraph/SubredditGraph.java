package no.simula.umod.redditgraph;


import com.opencsv.CSVWriter;
import org.apache.commons.compress.compressors.CompressorException;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultUndirectedWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import static no.simula.umod.redditgraph.ConsoleUtils.log;
import static no.simula.umod.redditgraph.ConsoleUtils.logDuration;

class SubRedditGraph {

    final Graph<String, Edge> g = new DefaultUndirectedWeightedGraph<>(Edge.class);

    public void createCountListFromCsv(File inputFile) throws IOException, CompressorException {
        long startTime = System.nanoTime();
        log("Start read csv to hashmap.");
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
                        edge.incrementNumberOfUsersInBothSubreddits();
                    } else {
                        g.addEdge(arr[i], arr[j], new Edge());
                    }
                }
            }
        });

        logDuration("Finished building the graph" + g.vertexSet().size(), startTime);
        log("# Vertices " + g.vertexSet().size());
        log("# Edges    " + g.edgeSet().size());
    }

    public void exportEdgeList(File outFile) throws IOException {
        final long startTime = System.nanoTime();
        final var writer = new FileWriter(outFile);
        final var buffer = new BufferedWriter(writer);
        final var csv = new CSVWriter(buffer);



        g.edgeSet().forEach(edge -> csv.writeNext(edge.getEdgeCsvLine()));

//        g.edgeSet().parallelStream().map(edge -> {
//            return edge.getEdgeCsvLine();
//        }).forEachOrdered(l -> {
//            System.out.println(Thread.currentThread());
//
//            csv.writeNext(l);
//        });

        csv.close();
        logDuration("Exported edge list", startTime);
    }

    public void exportDot(File outFile) throws IOException {
        final long startTime = System.nanoTime();
        final DOTExporter<String, Edge> exporter =
                new DOTExporter<>(v -> '"' + v.replace(".", "_") + '"');

        exporter.setEdgeAttributeProvider((edge) -> {
            final Map<String, Attribute> map = new LinkedHashMap<>();
            map.put("weight", DefaultAttribute.createAttribute(edge.getWeight()));

            return map;
        });

        final Writer writer = new FileWriter(outFile);

        exporter.exportGraph(g, writer);
        writer.close();
        logDuration("Exported dot", startTime);
    }

    class Edge extends DefaultWeightedEdge {


        private int numberOfUsersInThoseSubreddits = 1;
        private double weight;

        /**
         * Uij
         * @return
         */
        public int getNumberOfUsersInBothSubreddits() {
            return numberOfUsersInThoseSubreddits;
        }
        public void incrementNumberOfUsersInBothSubreddits() {
            numberOfUsersInThoseSubreddits++;
        }

        /**
         * i
         * @return
         */
        public String getSrc() {
            return this.getSource().toString();
        }

        /**
         * j
         * @return
         */
        public String getTar() {
            return this.getTarget().toString();
        }

        /**
         * degree i
         * @return
         */
        public int getSourceDegree() {
            return g.degreeOf((String) getSource());
        }

        /**
         * degree j
         * @return
         */
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
        private int getSumOfEdgeWeightsConnectedToVertex(final String vertex) {
            int sum = 0;

            //ToDo: This could be cached in the vertex.
            for (Edge edge : g.edgesOf(vertex)) {
                sum += edge.getNumberOfUsersInBothSubreddits();
            }
            return sum;
        }

        /**
         * Wij, stateful and cached. Works only if the weight has been calculated before.
         * @return
         */
        @Override
        public double getWeight() {
            return weight;
        }

        /**
         * Wij
         * @return
         */
        public double calculateWeight(final double avgi, final double avgj) {
            this.weight = (double)numberOfUsersInThoseSubreddits / (avgi + avgj);
            return this.weight;
        }


        //todo: vertex degree, and weighted degree file. plot distribution.


        public String[] getEdgeCsvLine() {
            final String source = getSrc();
            final String target = getTar();
            final int sourceDegree = g.degreeOf(source);
            final int targetDegree = g.degreeOf(target);
            final int weightedSourceDegree = getSumOfEdgeWeightsConnectedToVertex(source);
            final int weightedTargetDegree = getSumOfEdgeWeightsConnectedToVertex(target);
            final double weight = calculateWeight(weightedSourceDegree, weightedTargetDegree);

            return new String[]{
                    source,
                    target,
                    String.valueOf(numberOfUsersInThoseSubreddits),
                    String.valueOf(sourceDegree),
                    String.valueOf(targetDegree),
                    String.valueOf(weightedSourceDegree),
                    String.valueOf(weightedTargetDegree),
                    String.valueOf(weight)
            };
        }
    }
}

