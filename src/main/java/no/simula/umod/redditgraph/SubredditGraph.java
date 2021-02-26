package no.simula.umod.redditgraph;


import com.opencsv.CSVReader;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultUndirectedGraph;
import org.jgrapht.nio.GraphExporter;
import org.jgrapht.nio.dot.DOTExporter;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

class SubRedditGraph {

    final Graph<String, DefaultEdge> g = new DefaultUndirectedGraph<>(DefaultEdge.class);

    public void createGraphFromCSV() throws IOException {
        // todo: stream
        final var subredditUser = readAll("graphs/sample-user-in-sr.csv");

        final HashMap<String, HashSet<String>> users = new HashMap<>();

        for (final var entry : subredditUser) {
            // Create all vertices (duplicates handled by jgrapht)
            g.addVertex(entry[0]);

            // Create subreddit list per user
            users.putIfAbsent(entry[1], new HashSet<>());
            users.get(entry[1]).add(entry[0]);
        }

        // Add all the edges user -> subreddit x subreddit
        users.forEach((user, subreddits) -> {
            final String[] arr = new String[subreddits.size()];
            subreddits.toArray(arr);
            System.out.println("User:" + user + "----------------------------- subreddits: " + arr.length);

            for (int i = 0; i < arr.length; i++) {
                for (int j = i + 1; j < arr.length; j++) {
                    System.out.println(arr[i] + " -- " + arr[j]);

                    // Undirected unique edge per user
                    // Duplicated between the users possible, but jgrapht handles that for strings

                    g.addEdge(arr[i], arr[j]);
                }
            }
        });

        System.out.println("# Vertices " + g.vertexSet().size());
        System.out.println("# Edges    " + g.edgeSet().size());
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

    public void export() throws IOException {
        final GraphExporter<String, DefaultEdge> exporter =
                new DOTExporter<>(v -> v);
        //new GraphMLExporter<>(v -> v);

        final Writer writer = new FileWriter("graphs/sample.dot");

        exporter.exportGraph(g, writer);
        writer.close();
    }

    public List<String[]> readAll(String file) throws IOException {
        Reader reader = new FileReader(file);
        CSVReader csvReader = new CSVReader(reader);
        var list = csvReader.readAll();
        reader.close();
        csvReader.close();
        return list;
    }
}

