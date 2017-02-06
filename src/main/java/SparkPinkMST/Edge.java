package SparkPinkMST;

/**
 * Created by cjin on 4/10/14.
 */

public class Edge {
    private final int left;
    private final int right;
    private final double weight;

    Edge(int end1, int end2, double weight) {
        this.left = end1;
        this.right = end2;
        this.weight = weight;
    }

    int getLeft() {
        return left;
    }

    int getRight() {
        return right;
    }

    double getWeight() {
        return weight;
    }

    @Override
    public String toString() {
        return String.format("[(%d %d) %f]", left, right, weight);
    }
}
