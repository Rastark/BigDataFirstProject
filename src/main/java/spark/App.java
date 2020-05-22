package spark;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Launches MapJoinJob
 *
 */
public class App {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(final String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: <commandName> <file>");
            System.exit(1);
        }

        // Spark session creation
        SparkSession spark = SparkSession.builder().appName("BigDataProject").getOrCreate();

        // Import and Map creation
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        // Reduce creation
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        // Output Result
        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        // Spark Session termination

        spark.stop();
    }

    public JavaPairRDD<String, Integer> filterOnWordCount(JavaPairRDD<String, Integer> wordcounts, int x) {

        JavaPairRDD<String, Integer> filtered = wordcounts.filter(couple -> couple._2() > x);
        return filtered;
    };
}