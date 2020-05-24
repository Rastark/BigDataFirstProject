package spark;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
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
        SparkSession spark = SparkSession.builder().appName("BigDataProject").getOrCreate();

        // Import and Map creation
        JavaRDD<String> priceLines = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> stockLines = spark.read().textFile(args[1]).javaRDD();
        
        JavaRDD<StockPrice> stockPrices = StocksParser.parseFileLineToStockPrice(priceLines);  
        JavaRDD<StockName> stockNames = StocksParser.parseFileLineToStock(stockLines);

        //Join
        JavaPairRDD<String, StockName> stockNamesByTicker = stockNames.keyBy((StockName::getTicker));
        JavaPairRDD<String, StockPrice> stockPricesByTicker = stockPrices.keyBy((StockPrice::getTicker));
        JavaPairRDD<String, Tuple2<StockName, StockPrice>> stockNamePrices = stockNamesByTicker.join(stockPricesByTicker);

        JavaPairRDD<String, CompleteStock> completeStocksByTicker = stockNamePrices.mapValues(snp -> new CompleteStock(
            snp._1().getTicker(),
            snp._1().getName(),
            snp._1().getSector(),
            snp._2().getClose(),
            snp._2().getVolume(),
            snp._2().getDate()));
            
        //Job2a    
        JavaPairRDD<Tuple2<String, Integer>, CompleteStock> completeStocksBySectorDate = completeStocksByTicker
            .mapToPair(cst -> new Tuple2<>(new Tuple2<>(cst._2().getSector(), cst._2().getDate().getYear()), cst._2())); 

        JavaPairRDD<Tuple2<String, Date>, Tuple2<Long, Long>> volumesBySectorDate = completeStocksBySectorDate.mapValues(v -> new Tuple2<>(v.getVolume(), Long.valueOf(1))); 
        JavaPairRDD<Tuple2<String, Date>, Tuple2<Long, Long>> totalSectorDateVolumes = volumesBySectorDate.reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2()));
        JavaPairRDD<Tuple2<String, Date>, Double> meanSectorDateVolumes = totalSectorDateVolumes.mapValues(v -> Double.valueOf(v._1()) / v._2());    

        //Jon2b

    //     // Job1a
    //     JavaPairRDD<String, Tuple2<Double,Date>> tickerMap = stockPrices.mapToPair(sp -> new Tuple2<>(sp.getTicker(), new Tuple2<>(sp.getClose(), sp.getDate())));

    //     // Job1b
    //     JavaPairRDD<String, Tuple2<Double,Date>> minTickerDateClose = tickerMap.reduceByKey((mpv1, mpv2) -> minDateClose(mpv1, mpv2));
    //     JavaPairRDD<String, Tuple2<Double,Date>> maxTickerDateClose = tickerMap.reduceByKey((mpv1, mpv2) -> maxDateClose(mpv1, mpv2));

    //     JavaPairRDD<String, Tuple2<Tuple2<Double,Date>,Tuple2<Double,Date>>> joinTickerDateClose = minTickerDateClose.join(maxTickerDateClose);
        
    //     JavaPairRDD<String, Double> quotationChanges = joinTickerDateClose.mapValues(mpt -> quotationChange(mpt));

    //     // Job1c-e 
    //     JavaPairRDD<String, Double> lowPrices = stockPrices.mapToPair(sp -> new Tuple2<>(sp.getTicker(), sp.getLowThe()));
    //     JavaPairRDD<String, Double> highPrices = stockPrices.mapToPair(sp -> new Tuple2<>(sp.getTicker(), sp.getHighThe()));
        
    //     JavaPairRDD<String, Double> minTickerPrices = lowPrices.reduceByKey((mpv1, mpv2) -> Math.min(mpv1, mpv2));
    //     JavaPairRDD<String, Double> maxTickerPrices = highPrices.reduceByKey((mpv1, mpv2) -> Math.max(mpv1, mpv2));



    //     // Spark Session termination

    //     JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Double, Double>,Double>,Double>> result = 
    //         quotationChanges.join(minTickerPrices).join(maxTickerPrices).join(meanTickerVolumes);

    //     result.sortByKey().coalesce(1).saveAsTextFile(args[1]);

    //     spark.stop();
    // }

    // public static Tuple2<Double, Date> maxDateClose(Tuple2<Double, Date> t1, Tuple2<Double, Date> t2) { 
    //     return t1._2().compareTo(t2._2()) > 0 ? t1 : t2;
    // }

    // public static Tuple2<Double, Date> minDateClose(Tuple2<Double, Date> t1, Tuple2<Double, Date> t2) { 
    //     return t1._2().compareTo(t2._2()) < 0 ? t1 : t2;
    // }

    // public static Double quotationChange(Tuple2<Tuple2<Double, Date>, Tuple2<Double, Date>> t) {
    //     double firstClose = t._1._1();
    //     double lastClose = t._2._1();
    //     double quotationChangeDouble = (lastClose - firstClose) / firstClose * 100;
    //     return quotationChangeDouble;
    // }

    
    }
}