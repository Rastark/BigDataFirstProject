package spark;

import java.util.regex.Pattern;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import spark.dataframe.StockPrice;
import spark.utils.StocksParser;


public class Job1 {

    public static void main(final String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: <commandName> <file>");
            System.exit(1);
        }

        // Spark session creation
        SparkSession spark = SparkSession.builder().appName("BigDataProject").getOrCreate();

        // Import and Map creation
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        
        JavaRDD<StockPrice> stockPrices = StocksParser.parseFileLineToStockPrice(lines);  
        
        // Job1a
        JavaPairRDD<String, Tuple2<Double, Integer>> tickerMap = stockPrices.mapToPair(sp -> new Tuple2<>(sp.getTicker(), new Tuple2<>(sp.getClose(), sp.getYear())));

        // Job1b
        JavaPairRDD<String, Tuple2<Double, Integer>> minTickerDateClose = tickerMap.reduceByKey((mpv1, mpv2) -> minDateClose(mpv1, mpv2));
        JavaPairRDD<String, Tuple2<Double, Integer>> maxTickerDateClose = tickerMap.reduceByKey((mpv1, mpv2) -> maxDateClose(mpv1, mpv2));

        JavaPairRDD<String, Tuple2<Tuple2<Double, Integer>,Tuple2<Double, Integer>>> joinTickerDateClose = minTickerDateClose.join(maxTickerDateClose);
        
        JavaPairRDD<String, Double> quotationChanges = joinTickerDateClose.mapValues(mpt -> quotationChange(mpt));

        // Job1c-e 
        JavaPairRDD<String, Double> lowPrices = stockPrices.mapToPair(sp -> new Tuple2<>(sp.getTicker(), sp.getLowThe()));
        JavaPairRDD<String, Double> highPrices = stockPrices.mapToPair(sp -> new Tuple2<>(sp.getTicker(), sp.getHighThe()));
        
        JavaPairRDD<String, Double> minTickerPrices = lowPrices.reduceByKey((mpv1, mpv2) -> Math.min(mpv1, mpv2));
        JavaPairRDD<String, Double> maxTickerPrices = highPrices.reduceByKey((mpv1, mpv2) -> Math.max(mpv1, mpv2));

        //Job1f
        JavaPairRDD<String, Tuple2<Long, Integer>> volumes = stockPrices.mapToPair(sp -> new Tuple2<>(sp.getTicker(), new Tuple2<>(sp.getVolume(), Integer.valueOf(1))));    
        JavaPairRDD<String, Tuple2<Long, Integer>> totalTickerVolumes = volumes.reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2()));
        JavaPairRDD<String, Double> meanTickerVolumes = totalTickerVolumes.mapValues(v -> Double.valueOf(v._1())/ v._2());    

        // Spark Session termination

        JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Double, Double>,Double>,Double>> result = 
            quotationChanges.join(minTickerPrices).join(maxTickerPrices).join(meanTickerVolumes);

        result.sortByKey().coalesce(1).saveAsTextFile(args[1]);

        spark.stop();
    }
    public static Tuple2<Double, Integer> maxDateClose(Tuple2<Double, Integer> t1, Tuple2<Double, Integer> t2) {
        return t1._2().compareTo(t2._2()) > 0 ? t1 : t2;
    }

    public static Tuple2<Double, Integer> minDateClose(Tuple2<Double, Integer> t1, Tuple2<Double, Integer> t2) {
        return t1._2().compareTo(t2._2()) < 0 ? t1 : t2;
    }

    public static Double quotationChange(Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>> t) {
        double firstClose = t._1._1();
        double lastClose = t._2._1();
        double quotationChangeDouble = (lastClose - firstClose) / firstClose * 100;
        return quotationChangeDouble;
    }

}