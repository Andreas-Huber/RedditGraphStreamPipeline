package no.simula.umod.redditgraph;

import com.opencsv.bean.CsvBindByName;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultWeightedEdge;

public class Edge extends DefaultWeightedEdge implements ToCsv {
    private Graph<SrVertex, Edge> graph;

    @CsvBindByName(column = "i", required = true)
    protected String sourceName;                      // i
    @CsvBindByName(column = "j", required = true)
    protected String targetName;                      // j
    @CsvBindByName(column = "U_ij", required = true)
    private int numberOfUsersInThoseSubreddits = 1; // Uij

    @CsvBindByName(column = "degree_i", required = true)
    private int sourceDegree;                       // degree(i)
    @CsvBindByName(column = "degree_j", required = true)
    private int targetDegree;                       // degree(j)

    @CsvBindByName(column = "weighted-degree_i", required = true)
    private double weightedSourceDegree;            // weightedDegree(i)
    @CsvBindByName(column = "weighted-degree_j", required = true)
    private double weightedTargetDegree;            // weightedDegree(j)

    @CsvBindByName(column = "avg-weighted-edge-weight_i", required = true)
    private double avgWeightedSourceEdgeWeight;
    @CsvBindByName(column = "avg-weighted-edge-weight_j", required = true)
    private double avgWeightedTargetEdgeWeight;


    @CsvBindByName(column = "W_ij", required = true)
    private double weight;                          // Wij

    @SuppressWarnings("unused")  // Used for deserialization
    public Edge() {
    }

    public Edge(Graph<SrVertex, Edge> graph) {
        this.graph = graph;
    }

    public void setGraph(Graph<SrVertex, Edge> graph) {
        this.graph = graph;
    }

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
     * Increment Uij
     */
    public void incrementNumberOfUsersInBothSubreddits() {
        numberOfUsersInThoseSubreddits++;
    }


    /**
     * Wij, stateful and cached. Works only if the weight has been calculated or set before.
     */
    @Override
    public double getWeight() {
        return weight;
    }

    /**
     * Calculates weights after the vertex scores have ben calculated
     */
    public void calculateScoresDependentOnVertex() {
        final SrVertex source = getSrc();
        final SrVertex target = getTar();

        this.sourceName = source.subreddit;
        this.targetName = target.subreddit;
        this.sourceDegree = graph.degreeOf(source);
        this.targetDegree = graph.degreeOf(target);

        this.weightedSourceDegree = source.sumOfEdgeWeightsConnectedToVertex;
        this.weightedTargetDegree = target.sumOfEdgeWeightsConnectedToVertex;

        this.avgWeightedSourceEdgeWeight =
                (source.sumOfEdgeWeightsConnectedToVertex / (double) sourceDegree);

        this.avgWeightedTargetEdgeWeight =
                (target.sumOfEdgeWeightsConnectedToVertex / (double) targetDegree);

        calculateWeight(weightedSourceDegree, weightedTargetDegree);
    }

    /**
     * Wij
     */
    private void calculateWeight(final double avgi, final double avgj) {
        this.weight = (double) numberOfUsersInThoseSubreddits / ((avgi + avgj) / 2); // todo: (avgi + avgj) / 2 yes no???
    }

    public String[] toCsvLine() {
        return new String[]{
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
