package SparkPinkMST;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * Created by cjin on 4/27/14.
 */
public class DataSplitter {
    private static Logger LOGGER = LoggerFactory.getLogger(DataSplitter.class.getName());
    private final int numSplits;
    private final String inputFileName;
    private int numPoints;
    private int numDimension;
    private Double[][] data;
    private final String outputDir;
    private final JavaSparkContext sc;

    DataSplitter(String inputFileName, int numSplits, String outputDir, String configFile) throws Exception {
        this.inputFileName = inputFileName;
        this.numSplits = numSplits;
        this.outputDir = outputDir;
        this.sc = initSparkContext(configFile);
    }

    private static int swapBytesInt(int t) {
        return (0x000000ff & (t >> 24)) | (0x0000ff00 & (t >> 8)) | (0x00ff0000 & (t << 8)) | (0xff000000 & (t << 24));
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {

        // final String PROJECT_HOME = "/Users/cjin/Downloads/data/small";
        final String PROJECT_HOME = "hdfs://ec2-54-187-23-250.us-west-2.compute.amazonaws.com:9000/";
        // String inputFileName = "/Users/cjin/Downloads/data/datasets/clust32k.bin";
        String inputFileName = "/vol/experiements/clust128k.bin";

        int numSplits = 2;
        final String outputDir = PROJECT_HOME + "/dataPartitions";

        if (args.length == 2) {
            inputFileName = args[0];
            numSplits = Integer.parseInt(args[1]);
        } else if (args.length > 2) {
            System.err.format("Only %d parameters are given\n", args.length);
            for (int i = 0; i < args.length; i++) {
                System.err.println("arg " + i + ": " + args[i]);
            }
            System.err.println("Usage: java DataSplitter inputFile numPartitions <outDir>");
            System.exit(-1);
        }
        String configFile = "./sparkConfig";
        DataSplitter splitter = new DataSplitter(inputFileName, numSplits, outputDir, configFile);
        splitter.writeSequenceFiles(10, 5);
        splitter.readFile();
    }

    private static PathMatcher getGlobMatcher(String glob) {
        return FileSystems.getDefault().getPathMatcher("glob:" + glob);
    }

    private static void addJarPath(List<String> jarPaths, List<PathMatcher> exclusionPathMatchers, File jar) {
        if (isExcluded(exclusionPathMatchers, jar)) {
            LOGGER.debug("Excluding jar to send to workers: {}", jar);
        } else {
            LOGGER.debug("Including jar to send to workers: {}", jar);
            jarPaths.add(jar.getAbsolutePath());
        }
    }

    private static void addJarPaths(List<String> jarPaths, List<PathMatcher> exclusionPathMatchers,
            Path clientConfigPath, String jarPath) {
        Path resolvedJarPath = clientConfigPath.resolveSibling(jarPath);
        File jarFile = resolvedJarPath.toFile();
        if (jarFile.exists()) {
            if (jarFile.isDirectory()) {
                LOGGER.debug("Adding to SparkContext the jars from {}", resolvedJarPath);
                Iterator<File> jarsIterator = FileUtils.iterateFiles(jarFile, new String[] { "jar" }, true);
                while (jarsIterator.hasNext()) {
                    addJarPath(jarPaths, exclusionPathMatchers, jarsIterator.next());
                }
            } else {
                LOGGER.debug("Adding to SparkContext jar {}", resolvedJarPath);
                addJarPath(jarPaths, exclusionPathMatchers, jarFile);
            }
        } else if (!jarFile.getAbsolutePath().contains("*")) {
            LOGGER.debug("file doesn't exist and wasn't a glob containing '*': " + jarFile.getAbsolutePath());
        } else {
            Path parent = resolvedJarPath.getParent();
            File parentFile = parent.toFile();
            if (!parentFile.exists() || !parentFile.isDirectory()) {
                LOGGER.debug("parent path doesn't exist for glob: " + jarFile.getAbsolutePath());
            } else {
                LOGGER.debug("Adding to SparkContext the jars matching {}", resolvedJarPath);
                PathMatcher matcher = getGlobMatcher(resolvedJarPath.toAbsolutePath().toString());
                Iterator<File> jarsIterator = FileUtils.iterateFiles(parentFile, new String[] { "jar" }, true);
                while (jarsIterator.hasNext()) {
                    File loopFile = jarsIterator.next();
                    Path loopPath = FileSystems.getDefault().getPath(loopFile.getAbsolutePath());
                    if (matcher.matches(loopPath)) {
                        addJarPath(jarPaths, exclusionPathMatchers, loopFile);
                    }
                }
            }
        }
    }

    private static boolean isExcluded(List<PathMatcher> exclusionPatterns, File file) {
        for (PathMatcher exclusionPattern : exclusionPatterns) {
            if (exclusionPattern.matches(file.toPath().getFileName())) {
                return true;
            }
        }
        return false;
    }

    public JavaSparkContext initSparkContext(String configureFile) throws Exception {
        SparkConf sparkConf = new SparkConf(false);
        File file = new File(configureFile);
        if (configureFile != null && file.exists()) {
            Properties properties = new Properties();
            properties.load(new FileInputStream(file));

            sparkConf.setAppName("Pink");
            String masterUrl = properties.getProperty("pink.masterUrl");
            String sparkHome = properties.getProperty("pink.sparkHome");

            sparkConf.setMaster(masterUrl);
            sparkConf.setSparkHome(sparkHome);

            String jarPathsIn = properties.getProperty("pink.jarPaths", "");
            Iterable<String> jarPathsIterable = Splitter.on(',').split(jarPathsIn);
            String clientConfigString = properties.getProperty("pink.cli.dtclient.config", ".");
            Path clientConfigPath = FileSystems.getDefault().getPath(clientConfigString);

            List<PathMatcher> exclusionPathMatchers = Lists.newArrayList();
            for (String glob : jarPathsIterable) {
                exclusionPathMatchers.add(getGlobMatcher(glob));
            }

            // add jars
            List<String> jarPaths = Lists.newArrayList();
            for (String jarPath : jarPathsIterable) {
                addJarPaths(jarPaths, exclusionPathMatchers, clientConfigPath, jarPath);
            }

            for (String path : jarPaths) {
                System.out.println("addjar: " + path);
            }
            sparkConf.setJars(jarPaths.toArray(new String[jarPaths.size()]));

            for (Object key : properties.keySet()) {
                if (((String) key).startsWith("spark.")) {
                    sparkConf.set((String) key, (String) properties.get(key));
                }
            }
        }

        return new JavaSparkContext(sparkConf);
    }

    public JavaSparkContext getSparkContext() {
        return sc;
    }

    public void writeSequenceFiles(int numPoints, int numDimension) throws Exception {
        loadData();
        Point value = new Point();
        int pointId = 0;
        List<Point> points = Lists.newArrayListWithExpectedSize(numPoints);
        Double[] payload = new Double[numDimension];
        for (int i = 0; i < numPoints; i++) {
            payload = Arrays.copyOf(data[pointId], numDimension);
            value.set(pointId, payload);
            points.add(value.clone());
            pointId++;
        }
        displayValue(points, 5);
        saveAsSequenceFile(points);
    }

    public static void displayValue(List<Point> points, int size) {
        System.out.println("==========================================");
        int num = 0;
        for (Point p : points) {
            System.out.println("=====: " + p);
            if (++num > size) {
                break;
            }
        }
        System.out.println("==========================================");
    }

    public void writeSmallFiles() {
        List<Point> points = Lists.newArrayList(new Point(0, new Double[] { 0.0, 0.0 }), new Point(1, new Double[] {
                2.0, 0.0 }), new Point(2, new Double[] { 1.0, 2.0 }), new Point(3, new Double[] { 1.2, 2.0 }));
        saveAsSequenceFile(points);
    }

    public void readFile() throws Exception {
        @SuppressWarnings("unchecked")
        JavaRDD<Point> val = sc.hadoopFile(outputDir, SequenceFileInputFormat.class, NullWritable.class, Point.class)
                .map(new CloneFunction());
        List<Point> list = val.collect();
        PinkMST.displayValue(list, null, list.size());
    }

    public void createPartitionFiles(String fileLoc, int numPartitions) throws Exception {

        List<String> idSubgraphs = Lists.newArrayListWithCapacity(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            idSubgraphs.add(String.valueOf(i));
        }
        System.out.println("create idSubgraph files: " + fileLoc);
        try {
            sc.parallelize(idSubgraphs, numPartitions).saveAsTextFile(fileLoc);
        } catch (Exception e) {
            return;
        }
    }

    private static class CloneFunction implements Function<Tuple2<NullWritable, PointWritable>, Point> {

        private static final long serialVersionUID = 1L;

        @Override
        public Point call(Tuple2<NullWritable, PointWritable> row) throws Exception {
            return row._2.clone();
        }
    }

    private void saveAsSequenceFile(List<Point> points) {

        File outputFile = new File(outputDir);
        if (outputFile.exists()) {
            LOGGER.info("the directory exists: " + outputDir);
            delete(outputFile);
        }
        if (outputFile.exists()) {
            throw new RuntimeException("the directory still exists: " + outputDir);
        } else {
            LOGGER.info("the directory is deleted: " + outputDir);
        }

        JavaRDD<Point> pointsToWrite = sc.parallelize(points, numSplits);
        JavaPairRDD<NullWritable, PointWritable> pointsPairToWrite = pointsToWrite.mapToPair(new ToPairFunction());

        try {
            pointsPairToWrite.saveAsHadoopFile(outputDir, NullWritable.class, PointWritable.class,
                    SequenceFileOutputFormat.class);
        } catch (Exception e) {
            return;
        }

    }

    public static final class ToPairFunction implements PairFunction<Point, NullWritable, PointWritable> {

        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<NullWritable, PointWritable> call(Point row) throws CloneNotSupportedException {
            return new Tuple2<NullWritable, PointWritable>(NullWritable.get(), new PointWritable(row.clone()));
        }
    }

    private void loadData() throws IOException {
        int offset = 0;
        final int doubleSizeInBytes = Double.SIZE / 8;

        DataInputStream dataIn = new DataInputStream(new FileInputStream(new File(inputFileName)));
        numPoints = swapBytesInt(dataIn.readInt());
        numDimension = swapBytesInt(dataIn.readInt());
        System.out.println("data dimensions: " + numPoints + "," + numDimension);
        data = new Double[numPoints][numDimension];

        final int pointSizeInBytes = doubleSizeInBytes * numDimension;
        byte[] bytes = new byte[doubleSizeInBytes];
        byte[] result = new byte[pointSizeInBytes];

        for (int i = 0; i < numPoints; i++) {
            offset = 0;
            dataIn.read(result, 0, pointSizeInBytes);
            for (int j = 0; j < numDimension; j++) {
                System.arraycopy(result, offset, bytes, 0, doubleSizeInBytes);
                data[i][j] = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
                offset += doubleSizeInBytes;
            }
        }
        dataIn.close();
    }

    private static void delete(File file) {
        if (file.isDirectory()) {
            if (file.list().length == 0) {
                LOGGER.info("delete the empty Dir: " + file);
                file.delete();
            } else {
                for (String nestedFileName : file.list()) {
                    File nestedFile = new File(file, nestedFileName);
                    delete(nestedFile);
                }
                if (file.list().length == 0) {
                    file.delete();
                }
            }
        } else {
            LOGGER.info("delete the file: " + file);
            file.delete();
        }
    }
}
