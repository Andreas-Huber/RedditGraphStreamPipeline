package no.simula.umod.redditgraph;

import com.opencsv.bean.CsvBindByName;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;

public class SrVertex implements ToCsv {

    private Graph<SrVertex, Edge> graph;

    @CsvBindByName(column = "i", required = true)
    public String subreddit;
    @CsvBindByName(column = "Ui", required = true)
    public int sumOfEdgeWeightsConnectedToVertex = 0;
    @CsvBindByName(column = "degree-degree", required = true)
    private int degreeDegree;
    @CsvBindByName(column = "weighted-degree-degree", required = true)
    private double weightedDegreeDegree;
    @CsvBindByName(column = "local-clustering-coefficient", required = true)
    private double localClusteringCoefficient;

    @SuppressWarnings("unused") // Used for deserialization
    public SrVertex() {
    }

    public SrVertex(String subreddit, Graph<SrVertex, Edge> graph) {
        this.subreddit = subreddit;
        this.graph = graph;
    }

    public int getDegree() {
        return graph.degreeOf(this);
    }

    public double getWeightedDegree() {
        return sumOfEdgeWeightsConnectedToVertex / (double) graph.degreeOf(this);
    }

    public int getDegreeDegree() {
        return degreeDegree;
    }

    public double getWeightedDegreeDegree() {
        return weightedDegreeDegree;
    }

    public double getLocalClusteringCoefficient() {
        return localClusteringCoefficient;
    }

    public void setGraph(Graph<SrVertex, Edge> graph) {
        this.graph = graph;
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

    public void calculateIndependentScores() {
        this.sumOfEdgeWeightsConnectedToVertex = calculateSumOfEdgeWeightsConnectedToVertex();
    }

    public void calculateScoresDependentOnEdgeScores() {
        computeDegreeDegrees();
        this.localClusteringCoefficient = computeLocalClusteringCoefficient();
    }

    /**
     * Sum up all the edge weights connected a vertex
     */
    private int calculateSumOfEdgeWeightsConnectedToVertex() {
        int sum = 0;

        for (Edge edge : graph.edgesOf(this)) {
            sum += edge.getNumberOfUsersInBothSubreddits();
        }
        return sum;
    }

    private void computeDegreeDegrees() {
        int ddd = 0;
        double wdd = 0d;
        for (var vtx : Graphs.neighborListOf(graph, this)) {
            for (var edge : graph.edgesOf(vtx)) {
                wdd += edge.getWeight();
                ddd += 1;
            }
        }
        degreeDegree = ddd;
        weightedDegreeDegree = wdd;
    }

    private double computeLocalClusteringCoefficient() {
        final var neighbourhood = Graphs.neighborSetOf(graph, this);
        final double k = neighbourhood.size();
        double numberTriplets = 0;
        for (var p : neighbourhood)
            for (var q : neighbourhood)
                if (graph.containsEdge(p, q))
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
