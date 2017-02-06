package SparkPinkMST;

import com.google.common.collect.Lists;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by cjin on 4/12/14.
 */

public class PinkMST {

    private final int partitionId;
    private final List<Point> leftData;
    private final List<Point> rightData;
    private final boolean isBipartite;

    private JavaPairRDD<Tuple2<Integer, Double>, Iterable<Edge>> mstToBeMerged;

    PinkMST(Tuple2<List<Point>, List<Point>> pairedData, int partitionId) throws IOException {
        this.leftData = pairedData._1;
        this.rightData = pairedData._2;
        this.partitionId = partitionId;
        if (rightData == null) {
            this.isBipartite = false;
        } else {
            this.isBipartite = true;
        }
//        if (this.isBipartite) {
//            System.out.println("leftData/rightData:  " + this.leftData.size() + ", " + this.rightData.size());
//        } else {
//            System.out.println("leftData:  " + this.leftData.size());
//        }
//        displayValue(leftData, rightData, leftData.size());
    }

    public JavaRDD<Iterable<Edge>> getMST() {
        return mstToBeMerged.values();
    }

    public List<Tuple2<Integer, Edge>>getEdgeList() throws IOException {
        List<Tuple2<Integer, Edge>> edgeList;
        if (isBipartite) {
            edgeList = BipartiteMST();
        } else {
            edgeList = PrimLocal();
        }
        return edgeList;
    }

    private List<Tuple2<Integer, Edge>> BipartiteMST() {
        Point[] data1 = leftData.toArray(new Point[leftData.size()]);
        Point[] data2 = rightData.toArray(new Point[rightData.size()]);
        Point[] localLeftData;
        Point[] localRightData;

        int numLeftPoints = leftData.size();
        int numRightPoints = rightData.size();
        int numLeftPointsInTrack = 0;
        int numRightPointsInTrack = 0;

        List<Tuple2<Integer, Edge>> edgePairs = Lists.newArrayListWithCapacity(numLeftPoints + numRightPoints - 1);

        int[] next1 = new int[numLeftPoints];
        int[] parent1 = new int[numLeftPoints];
        double[] key1 = new double[numLeftPoints];

        int[] next2 = new int[numRightPoints];
        int[] parent2 = new int[numRightPoints];
        double[] key2 = new double[numRightPoints];

        int[] nextLeft = null, nextRight = null;
        int[] parentLeft = null, parentRight = null;
        double[] keyLeft = null, keyRight = null;

        Point[] tmpData;
        double[] tmpKey;
        int[] tmpNext, tmpParent;
        int tmpNumPoints = 0;
        // edges = new EdgeWritable[npts1 + npts2 - 1];
        // /////////////////////////////
        for (int i = 0; i < numLeftPoints; i++) {
            key1[i] = Double.MAX_VALUE;
            next1[i] = i;
            parent1[i] = -1;
        }

        for (int i = 0; i < numRightPoints; i++) {
            key2[i] = Double.MAX_VALUE;
            next2[i] = i;
            parent2[i] = -1;
        }

        if (numLeftPoints <= numRightPoints) {
            numLeftPointsInTrack = numLeftPoints;
            numRightPointsInTrack = numRightPoints;

            keyLeft = key1;
            nextLeft = next1;
            parentLeft = parent1;

            keyRight = key2;
            nextRight = next2;
            parentRight = parent2;

            localLeftData = data1;
            localRightData = data2;
        } else {
            numLeftPointsInTrack = numRightPoints;
            numRightPointsInTrack = numLeftPoints;

            keyLeft = key2;
            nextLeft = next2;
            parentLeft = parent2;

            keyRight = key1;
            nextRight = next1;
            parentRight = parent1;

            localLeftData = data2;
            localRightData = data1;
        }

        parentLeft[0] = -1;
        int next = 0, shift, currPoint = 0, otherPoint = 0;

        double minV = 0, dist = 0;
        boolean isSwitch = true;
        int gnextParent = 0, gnext = 0;
        while (numRightPointsInTrack > 0) {
            shift = 0;
            currPoint = next;
            next = nextRight[shift];
            minV = Double.MAX_VALUE;
            for (int i = 0; i < numRightPointsInTrack; i++) {
                isSwitch = true;
                otherPoint = nextRight[i];
                try {
                    dist = localLeftData[currPoint].distanceTo(localRightData[otherPoint]);
                    if (keyRight[otherPoint] > dist) {
                        keyRight[otherPoint] = dist;
                        parentRight[otherPoint] = currPoint;
                    }
                } catch (Exception e) {
                    System.err.println("curr, other: " + currPoint + ", " + otherPoint);
                    e.printStackTrace();
                }

                if (keyRight[otherPoint] < minV) {
                    shift = i;
                    minV = keyRight[otherPoint];
                    next = otherPoint;
                }

                if (dist < keyLeft[currPoint]) {
                    keyLeft[currPoint] = dist;
                    parentLeft[currPoint] = otherPoint;
                }
            } // end of i
            gnext = localRightData[next].getId();
            gnextParent = localLeftData[parentRight[next]].getId();
            // find the global min
            for (int i = 0; i < numLeftPointsInTrack; i++) {
                currPoint = nextLeft[i];
                if (minV > keyLeft[currPoint]) {
                    isSwitch = false;
                    minV = keyLeft[currPoint];
                    otherPoint = parentLeft[currPoint];
                    gnextParent = localLeftData[currPoint].getId();
                    gnext = localRightData[otherPoint].getId();
                    next = currPoint;
                    shift = i;
                }
            }
            for (int i = 0; i < numLeftPointsInTrack; i++) {
                currPoint = nextLeft[i];
                otherPoint = parentLeft[currPoint];
            }
            if (numLeftPointsInTrack == numLeftPoints && numRightPointsInTrack == numRightPoints) {
                nextLeft[0] = nextLeft[--numLeftPointsInTrack];
            }
            Edge edge = new Edge(Math.min(gnext, gnextParent), Math.max(gnext, gnextParent), minV);
            edgePairs.add(new Tuple2<Integer, Edge> (this.partitionId, edge));
            if (!isSwitch) {
                nextLeft[shift] = nextLeft[--numLeftPointsInTrack];
                continue;
            }
            nextRight[shift] = nextRight[--numRightPointsInTrack];

            tmpData = localRightData;
            localRightData = localLeftData;
            localLeftData = tmpData;

            // swap keyLeft and keyRight
            tmpKey = keyRight;
            keyRight = keyLeft;
            keyLeft = tmpKey;

            // swap parentLeft and parentRight
            tmpParent = parentRight;
            parentRight = parentLeft;
            parentLeft = tmpParent;

            // swap nextLeft and nextRight
            tmpNext = nextRight;
            nextRight = nextLeft;
            nextLeft = tmpNext;

            // swap nptsLeft and nptsRight
            tmpNumPoints = numRightPointsInTrack;
            numRightPointsInTrack = numLeftPointsInTrack;
            numLeftPointsInTrack = tmpNumPoints;
        }

        for (int i = 0; i < numLeftPointsInTrack; i++) {
            currPoint = nextLeft[i];
            otherPoint = parentLeft[currPoint];
            minV = keyLeft[currPoint];
            gnextParent = localLeftData[currPoint].getId();
            gnext = localRightData[otherPoint].getId();
            Edge edge = new Edge(Math.min(gnext, gnextParent), Math.max(gnext, gnextParent), minV);
            edgePairs.add(new Tuple2<Integer, Edge> (this.partitionId, edge));
        }
        Collections.sort(edgePairs, new MyEdgeComparable());
        System.out.println("edgePairs: " + edgePairs.size());
        return edgePairs;
    }

    public List<Tuple2<Integer, Edge>> PrimLocal() throws IOException {
        Point[] data = leftData.toArray(new Point[leftData.size()]);
        int numPoints = leftData.size();
        List<Tuple2<Integer, Edge>> edgePairs = Lists.newArrayListWithCapacity(numPoints);

        int[] left = new int[numPoints];
        int[] parent = new int[numPoints];
        double[] key = new double[numPoints];

        for (int i = 0; i < numPoints; i++) {
            key[i] = Double.MAX_VALUE;
            left[i] = i + 1;
        }
        key[0] = 0;
        parent[0] = -1;

        int next = 0, shift, currPt, otherPt;
        double minV, dist = 0;
        // ///////////////////////////////////
        // iterate through all the data points
        // ///////////////////////////////////
        for (int j = numPoints - 1; j > 0;) {
            currPt = next;
            shift = 0;
            next = left[shift];
            minV = Double.MAX_VALUE;
            for (int i = 0; i < j; i++) {
                otherPt = left[i];
                try {
                    dist = data[currPt].distanceTo(data[otherPt]);
                    if (dist < key[otherPt]) {
                        key[otherPt] = dist;
                        parent[otherPt] = currPt;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (key[otherPt] < minV) {
                    shift = i;
                    minV = key[otherPt];
                    next = otherPt;
                }
            }
            int globalNext = data[next].getId();
            int globalNextParent = data[parent[next]].getId();
            Edge edge = new Edge(Math.min(globalNext, globalNextParent), Math.max(globalNext, globalNextParent), minV);
            edgePairs.add(new Tuple2<Integer, Edge> (this.partitionId, edge));
            left[shift] = left[--j];
        }

        Collections.sort(edgePairs, new MyEdgeComparable());
//        System.out.println("edgePairs: " + edgePairs.size());
        return edgePairs;
    }

    private class MyEdgeComparable implements Comparator<Tuple2<Integer, Edge>> {
        @Override
        public int compare(Tuple2<Integer, Edge> t1, Tuple2<Integer, Edge> t2) {
            Edge e1 = t1._2;
            Edge e2 = t2._2;
            return (e1.getWeight() < e2.getWeight()) ? -1 : (e1.getWeight() == e2.getWeight()) ? 0 : 1;
        }
    }

    public static void displayValue(List<?> leftData, List<?> rightData, int numPoints) {
        for (int i = 0; i < numPoints; i++) {
            System.out.println("==== samples " + i + ", " + leftData.get(i) + " || "
                    + (rightData != null ? rightData.get(i) : null));
        }
    }
}
