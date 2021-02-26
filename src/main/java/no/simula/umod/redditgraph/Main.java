package no.simula.umod.redditgraph;
import com.opencsv.CSVReader;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultUndirectedGraph;
import org.jgrapht.nio.GraphExporter;
import org.jgrapht.nio.dot.DOTExporter;
import org.jgrapht.nio.graphml.GraphMLExporter;

import java.io.*;
import java.util.*;


class SubRedditGraph {

    Graph<String, DefaultEdge> g = new DefaultUndirectedGraph<>(DefaultEdge.class);
    Set<String> exists = new HashSet<>();

//        class SubReddit {
//            // Properties
//        }
//
//        class Edge {
//            // Properties
//        }

//        void addTuple(String userName, String subRedditName) {
//
//            SubReddit sub;
//            if (!exists.contains(subRedditName)) {
//                sub = new SubReddit();
//                exists.add(subRedditName);
//                graph.addVertex(sub);
//            }
//
//        }

//    public void CreateGraphFromCSV(){
//
//    }

    public void CreateSample(){


        var a = "politics";
        var b = "nsfw";
        var c = "thedonald";

        g.addVertex(a);
        g.addVertex(b);
        g.addVertex(c);
        g.addVertex(c);

        g.addEdge(a, c);
        g.addEdge(a, c);
        g.addEdge(a, c);
        g.addEdge(a, c);
        g.addEdge(b, c);
    }

    public void Export() throws IOException {
        GraphExporter<String, DefaultEdge> exporter =
                new DOTExporter<>(v -> v);
                //new GraphMLExporter<>(v -> v);

        Writer writer = new FileWriter("graphs/sample.dot");

        exporter.exportGraph(g, writer);
        System.out.println(writer.toString());

        writer.close();
    }

    public List<String[]> readAll(String file) throws Exception {
        Reader reader = new FileReader(file);
        CSVReader csvReader = new CSVReader(reader);
        var list = csvReader.readAll();
        reader.close();
        csvReader.close();
        return list;
    }
}

public class Main {


    public static void main(String[] args) throws IOException {
        var graph = new SubRedditGraph();

        graph.CreateSample();
        graph.Export();

    }
}
