package spark;

import java.util.regex.Pattern;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import spark.dataframe.*;
import spark.parser.StocksParser;

public class Job2 {

    private static final Pattern SPACE = Pattern.compile("\t");

    public static void main(final String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: <commandName> <file>");
            System.exit(1);
        }

        // Spark session creation
        SparkSession spark = SparkSession.builder().appName("BigDataProjectJob2").getOrCreate();

        // Import and Map creation
        JavaRDD<String> priceLines = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<StockPrice> stockPrices = StocksParser.parseFileLineToStockPrice(priceLines);

        JavaRDD<String> stockLines = spark.read().textFile(args[1]).javaRDD();
        JavaRDD<StockName> stockNames = StocksParser.parseFileLineToStock(stockLines);

        // Join
        JavaPairRDD<String, StockName> stockNamesByTicker = stockNames.mapToPair(v -> new Tuple2<>(v.getTicker(), v));
        JavaPairRDD<String, StockPrice> stockPricesByTicker = stockPrices.mapToPair(v -> new Tuple2<>(v.getTicker(), v));
        JavaPairRDD<String, Tuple2<StockName, StockPrice>> stockNamePrices = stockNamesByTicker
                .join(stockPricesByTicker);

        JavaPairRDD<String, CompleteStock> completeStocksByTicker = stockNamePrices
                .mapValues(snp -> new CompleteStock(snp._1().getTicker(), snp._1().getName(), snp._1().getSector(),
                        snp._2().getClose(), snp._2().getVolume(), snp._2().getDate()));

        JavaPairRDD<Tuple2<String, Integer>, CompleteStock> completeStocksBySectorYear = completeStocksByTicker
                .mapToPair(cst -> new Tuple2<>(new Tuple2<>(cst._2().getSector(), cst._2().getYear()), cst._2()));

        // Job2a
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Long, Long>> volumesBySectorYear = completeStocksBySectorYear
                .mapValues(v -> new Tuple2<>(v.getVolume(), Long.valueOf(1)));
        
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Long, Long>> totalSectorDateVolumes = volumesBySectorYear
                .reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2()));
        
        JavaPairRDD<Tuple2<String, Integer>, Double> meanSectorDateVolumes = totalSectorDateVolumes
                .mapValues(v -> Double.valueOf(v._1()) / v._2());

        // Jon2b
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, String>> dateCloseBySectorYear = completeStocksBySectorYear
                .mapValues(cssy -> new Tuple2<>(cssy.getClose(), cssy.getDate()));

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, String>> minDateCloseBySectorYear = dateCloseBySectorYear
                .reduceByKey((v1, v2) -> minDateClose(v1, v2));

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, String>> maxDateCloseBySectorYear = dateCloseBySectorYear
                .reduceByKey((v1, v2) -> maxDateClose(v1, v2));

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Tuple2<Double, String>, Tuple2<Double, String>>> joinDateCloseBySectorYear = minDateCloseBySectorYear
                .join(maxDateCloseBySectorYear);

        JavaPairRDD<Tuple2<String, Integer>, Double> meanQuotationChanges = joinDateCloseBySectorYear
                .mapValues(v -> new Tuple2<>(quotationChange(v), Long.valueOf(1)))
                .reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2()))
                .mapValues(v -> v._1() / v._2());

        // Job2c
        JavaPairRDD<Tuple2<String, Integer>, Double> meanDailyClose = dateCloseBySectorYear
                .mapValues(v -> new Tuple2<>(v._1(), Long.valueOf(1)))
                .reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2()))
                .mapValues(v -> v._1() / v._2());

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Tuple2<Double, Double>, Double>> result = meanSectorDateVolumes
            .join(meanQuotationChanges).join(meanDailyClose);

        result.coalesce(1).saveAsTextFile(args[2]);

        spark.stop();

    }

    // Spark Session termination
    public static Tuple2<Double, String> maxDateClose(Tuple2<Double, String> t1, Tuple2<Double, String> t2) {
        return t1._2().compareTo(t2._2()) > 0 ? t1 : t2;
    }

    public static Tuple2<Double, String> minDateClose(Tuple2<Double, String> t1, Tuple2<Double, String> t2) {
        return t1._2().compareTo(t2._2()) < 0 ? t1 : t2;
    }

    public static Double quotationChange(Tuple2<Tuple2<Double, String>, Tuple2<Double, String>> t) {
        double firstClose = t._1._1();
        double lastClose = t._2._1();
        double quotationChangeDouble = (lastClose - firstClose) / firstClose * 100;
        return quotationChangeDouble;
    }

}