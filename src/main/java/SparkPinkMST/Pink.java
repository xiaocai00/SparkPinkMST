package SparkPinkMST;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.collect.Lists;

/**
 * Driver program Created by cjin on 4/10/14.
 */
public class Pink {

    public static void main(String[] args) throws Exception {
        int K = 3;
        int numDataSplits = 2;
        int numPoints = 32 * 1024;
        int numDimensions = 10;
        String dataSetName = "clust32k";
        String configFile = null;
        String PROJECT_HOME = "/Users/cjin/Downloads/data/";
        String FS_PREFIX = "file://";
        if (args.length == 8) {
            dataSetName = args[0];
            numPoints = Integer.parseInt(args[1]);
            numDimensions = Integer.parseInt(args[2]);
            numDataSplits = Integer.parseInt(args[3]);
            K = Integer.parseInt(args[4]);
            configFile = args[5];
            PROJECT_HOME = args[6];
            FS_PREFIX = args[7];
            System.out.format(
                    "PinkConfig: dataSet=%s numPoints=%d numDimensions=%d numDataSplits=%d K=%d configFile=%s\n",
                    dataSetName, numPoints, numDimensions, numDataSplits, K, configFile);
        }

        int numSubGraphs = numDataSplits * (numDataSplits - 1) / 2 + numDataSplits;

        final String EXPERIMENT_HOME = FS_PREFIX + dataSetName + "_d" + numDimensions + "_s" + numDataSplits;

        if (FS_PREFIX.startsWith("file")) {
            final File dir = new File(EXPERIMENT_HOME);
            dir.mkdir();
        }
        // number of subgraphs
        final String idPartitionFilesLoc = EXPERIMENT_HOME + "/subgraphIds";
        // number of dataPartitions
        final String dataParitionFilesLoc = EXPERIMENT_HOME + "/dataPartitions";

        String binaryFileName = PROJECT_HOME + dataSetName + ".bin";
        DataSplitter splitter = new DataSplitter(binaryFileName, numDataSplits, dataParitionFilesLoc, configFile);
        splitter.createPartitionFiles(idPartitionFilesLoc, numSubGraphs);
        splitter.writeSequenceFiles(numPoints, numDimensions);
        JavaSparkContext sc = splitter.getSparkContext();

        JavaRDD<String> partitionRDD = sc.textFile(idPartitionFilesLoc, numSubGraphs);
        long start = System.currentTimeMillis();
        JavaPairRDD<Integer, Edge> partitions = partitionRDD.flatMapToPair(new GetPartitionFunction(
                dataParitionFilesLoc, numDataSplits));
        // how to parallelize the edgePairList
        // create the collection of edges for the Kruskal Merge
        JavaPairRDD<Integer, Iterable<Edge>> mstToBeMerged = partitions.combineByKey(new CreateCombiner(),
                new Merger(), new KruskalReducer(numPoints));
        JavaPairRDD<Integer, Iterable<Edge>> mstToBeMergedResult = null;
        while (numSubGraphs > 1) {
            numSubGraphs = (numSubGraphs + (K - 1)) / K;
            mstToBeMergedResult = mstToBeMerged.mapToPair(new SetPartitionIdFunction(K)).reduceByKey(
                    new KruskalReducer(numPoints), numSubGraphs);
            mstToBeMerged = mstToBeMergedResult;
        }

        if (mstToBeMergedResult.splits().size() != 1) {
            throw new RuntimeException("the split Size is not one");
        }

        displayResults(mstToBeMerged);
        long end = System.currentTimeMillis();
        System.out.println("PinkTotalTime=" + (end - start));

    }

    private static void displayResults(JavaPairRDD<Integer, Iterable<Edge>> mstToBeMerged) {
        System.out.println(mstToBeMerged.count());
        // int numEdges = 0;
        // for (Tuple2<Integer, Iterable<Edge>> record : mstToBeMerged.collect()) {
        // System.out.println("===== key : " + record._1);
        // for (Edge edge : record._2) {
        // if (++numEdges > 10) {
        // break;
        // }
        // System.out.println("===== edge : " + edge);
        // }
        // System.out.println();
        //
        // }
    }

    private static class KruskalReducer implements Function2<Iterable<Edge>, Iterable<Edge>, Iterable<Edge>> {

        private static final long serialVersionUID = 1L;

        private transient UnionFind uf = null;
        private final int numPoints;

        public KruskalReducer(int numPoints) {
            this.numPoints = numPoints;
        }

        // merge sort
        @Override
        public Iterable<Edge> call(Iterable<Edge> leftEdges, Iterable<Edge> rightEdges) throws Exception {
            uf = new UnionFind(numPoints);
            List<Edge> edges = Lists.newArrayList();
            Iterator<Edge> leftEdgesIterator = leftEdges.iterator();
            Iterator<Edge> rightEdgesIterator = rightEdges.iterator();
            Edge leftEdge = leftEdgesIterator.next();
            Edge rightEdge = rightEdgesIterator.next();
            Edge minEdge;
            boolean isLeft;
            Iterator<Edge> minEdgeIterator;
            final int numEdges = numPoints - 1;
            do {
                if (leftEdge.getWeight() < rightEdge.getWeight()) {
                    minEdgeIterator = leftEdgesIterator;
                    minEdge = leftEdge;
                    isLeft = true;
                } else {
                    minEdgeIterator = rightEdgesIterator;
                    minEdge = rightEdge;
                    isLeft = false;
                }
                if (uf.unify(minEdge.getLeft(), minEdge.getRight())) {
                    edges.add(minEdge);
                }
                minEdge = minEdgeIterator.hasNext() ? minEdgeIterator.next() : null;
                if (isLeft) {
                    leftEdge = minEdge;
                } else {
                    rightEdge = minEdge;
                }
            } while (minEdge != null && edges.size() < numEdges);
            minEdge = isLeft ? rightEdge : leftEdge;
            minEdgeIterator = isLeft ? rightEdgesIterator : leftEdgesIterator;

            while (edges.size() < numEdges && minEdgeIterator.hasNext()) {
                if (uf.unify(minEdge.getLeft(), minEdge.getRight())) {
                    edges.add(minEdge);
                }
                minEdge = minEdgeIterator.next();
            }
            return edges;
        }
    }

    private static class CreateCombiner implements Function<Edge, Iterable<Edge>> {

        private static final long serialVersionUID = 1L;

        @Override
        public Iterable<Edge> call(Edge edge) throws Exception {
            List<Edge> edgeList = Lists.newArrayListWithCapacity(1);
            edgeList.add(edge);
            return edgeList;
        }
    }

    private static class Merger implements Function2<Iterable<Edge>, Edge, Iterable<Edge>> {

        private static final long serialVersionUID = 1L;

        @Override
        public Iterable<Edge> call(Iterable<Edge> list, Edge edge) throws Exception {
            List<Edge> mergeList = Lists.newArrayList(list);
            mergeList.add(edge);
            return mergeList;
        }
    }

    private static class SetPartitionIdFunction implements
            PairFunction<Tuple2<Integer, Iterable<Edge>>, Integer, Iterable<Edge>> {

        private static final long serialVersionUID = 1L;

        private final int K;

        SetPartitionIdFunction(int K) {
            this.K = K;
        }

        @Override
        public Tuple2<Integer, Iterable<Edge>> call(Tuple2<Integer, Iterable<Edge>> row) {
            Integer key = row._1 / K;
            return new Tuple2<Integer, Iterable<Edge>>(key, row._2);
        }
    }

    public static final class GetPartitionFunction implements PairFlatMapFunction<String, Integer, Edge> {

        private static final long serialVersionUID = 1L;

        private final String inputDataFilesLoc;
        private final int numDataSplits;

        public GetPartitionFunction(String inputDataFilesLoc, int numDataSplits) {
            this.inputDataFilesLoc = inputDataFilesLoc;
            this.numDataSplits = numDataSplits;
        }

        @Override
        public Iterable<Tuple2<Integer, Edge>> call(String row) throws Exception {
            final int partionId = Integer.parseInt(row);
            Tuple2<List<Point>, List<Point>> subGraphsPair = getSubGraphPair(partionId, numDataSplits,
                    inputDataFilesLoc);
            PinkMST pinkMst = new PinkMST(subGraphsPair, partionId);
            List<Tuple2<Integer, Edge>> edgeList = pinkMst.getEdgeList();
            return edgeList;
        }

    }

    private static List<Point> openFile(String fileName) throws Exception {
        final Configuration conf = new Configuration();
        org.apache.hadoop.io.SequenceFile.Reader.Option fileOpt = Reader.file(new Path(fileName));
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, fileOpt);
        NullWritable key = NullWritable.get();
        PointWritable value = new PointWritable();
        List<Point> list = Lists.newArrayList();
        while (reader.next(key, value)) {
            list.add(value.clone());
        }

        reader.close();
        return list;
    }

    private static Tuple2<List<Point>, List<Point>> getSubGraphPair(int partitionId, int numDataSplits,
            String inputDataFilesLoc) throws Exception {
        int rightId = -1;
        int leftId = -1;
        int numBipartiteSubgraphs = numDataSplits * (numDataSplits - 1) / 2;
        if (partitionId < numBipartiteSubgraphs) {
            rightId = getRightId(partitionId);
            leftId = getLeftId(partitionId, rightId);
        } else {
            leftId = partitionId - numBipartiteSubgraphs;
            rightId = leftId;
        }
        String leftFileName = String.format("%s/part-%05d", inputDataFilesLoc, leftId);
        String rightFileName = String.format("%s/part-%05d", inputDataFilesLoc, rightId);
        List<Point> pointsLeft = openFile(leftFileName);
        List<Point> pointsRight = null;
        if (!leftFileName.equals(rightFileName)) {
            pointsRight = openFile(rightFileName);
        }
        return new Tuple2<List<Point>, List<Point>>(pointsLeft, pointsRight);
    }

    private static int getRightId(int partId) {
        return (((int) Math.sqrt((partId << 3) + 1) + 1) >> 1);
    }

    private static int getLeftId(int partId, int rightId) {
        return (partId - (((rightId - 1) * rightId) >> 1));
    }
}
