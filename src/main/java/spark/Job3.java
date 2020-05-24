package spark;

import java.util.Date;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;
import spark.dataframe.CompleteStock;
import spark.dataframe.StockName;
import spark.dataframe.StockPrice;
import spark.parser.StocksParser;

public class Job3 {

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
            snp._1().getTicker(),
            snp._2().getClose(),
            snp._2().getVolume(),
            snp._2().getDate()
        ));
        
        JavaPairRDD<Tuple2<String, Integer>, CompleteStock> completeStocksByTickerYear = completeStocksByTicker
        .mapToPair(cst -> new Tuple2<>(new Tuple2<>(cst._2().getTicker(), cst._2().getYear()), cst._2()));

        JavaPairRDD<Tuple2<String, Integer>, Tuple3<String, Double, String>> dateCloseByTickerYear = completeStocksByTickerYear
                .mapValues(cssy -> new Tuple3<>(cssy.getName(), cssy.getClose(), cssy.getDate()));

        JavaPairRDD<Tuple2<String, Integer>, Tuple3<String, Double, String>> minDateCloseByTickerYear = dateCloseByTickerYear
                .reduceByKey((v1, v2) -> minDateClose3(v1, v2));

        JavaPairRDD<Tuple2<String, Integer>, Tuple3<String, Double, String>> maxDateCloseByTickerYear = dateCloseByTickerYear
                .reduceByKey((v1, v2) -> maxDateClose3(v1, v2));

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Tuple3<String, Double, String>, Tuple3<String, Double, String>>> joinDateCloseByTickerYear = minDateCloseByTickerYear
                .join(maxDateCloseByTickerYear);

        JavaPairRDD<Tuple2<String, Integer>, Double> quotationChanges = joinDateCloseByTickerYear
            .mapValues(mpt -> quotationChange(mpt));

        // Jon2b

        JavaPairRDD<Tuple2<String, Integer>, Double> meanQuotationChanges = joinDateCloseByTickerYear
                .mapValues(v -> new Tuple2<>(quotationChange(v), Long.valueOf(1)))
                .reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2()))
                .mapValues(v -> v._1() / v._2());
    }

    public static Tuple3<String, Double, String> maxDateClose3(Tuple3<String, Double, String> t1, Tuple3<String, Double, String> t2) {
        return t1._3().compareTo(t2._3()) > 0 ? t1 : t2;
    }

    public static Tuple3<String, Double, String> minDateClose3(Tuple3<String, Double, String> t1, Tuple3<String, Double, String> t2) {
        return t1._3().compareTo(t2._3()) < 0 ? t1 : t2;
    }

    public static Double quotationChange(Tuple2<Tuple3<String, Double, String>, Tuple3<String, Double, String>> t) {
        double firstClose = t._1._2();
        double lastClose = t._2._2();
        double quotationChangeDouble = (lastClose - firstClose) / firstClose * 100;
        return quotationChangeDouble;
    }
    
}