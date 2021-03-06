package spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.Tuple3;
import spark.dataframe.CompleteStock;
import spark.dataframe.StockName;
import spark.dataframe.StockPrice;
import spark.utils.SerializableComparator;
import spark.utils.StocksParser;

public class Job3 {

    public static void main(final String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: <commandName> <file>");
            System.exit(1);
        }

        // Spark session creation
        SparkSession spark = SparkSession.builder().appName("BigDataProjectJob3").getOrCreate();

        // Import and Map creation
        JavaRDD<String> priceLines = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> stockLines = spark.read().textFile(args[1]).javaRDD();
        
        JavaRDD<StockPrice> stockPrices = StocksParser.parseFileLineToStockPrice(priceLines);  
        JavaRDD<StockName> stockNames = StocksParser.parseFileLineToStock(stockLines);
        
        //Join
        JavaPairRDD<String, StockName> stockNamesByTicker = stockNames.keyBy((StockName::getTicker)).persist(StorageLevel.MEMORY_ONLY_SER());
        JavaPairRDD<String, StockPrice> stockPricesByTicker = stockPrices.keyBy((StockPrice::getTicker));
        JavaPairRDD<String, Tuple2<StockName, StockPrice>> stockNamePrices = stockNamesByTicker.join(stockPricesByTicker);

        JavaPairRDD<String, CompleteStock> completeStocksByTicker = stockNamePrices.mapValues(snp -> new CompleteStock(
            snp._1().getTicker(),
            snp._1().getName(),
            snp._1().getTicker(),
            snp._2().getClose(),
            snp._2().getVolume(),
            snp._2().getYear(),
            snp._2().getDay()
        ));
        
        // Indicizzo per ticker e anno
        JavaPairRDD<Tuple2<String, Integer>, CompleteStock> completeStocksByTickerYear = completeStocksByTicker
            .mapToPair(cst -> new Tuple2<>(new Tuple2<>(cst._2.getTicker(), cst._2.getYear()), cst._2()))
            .filter(v-> v._1._2()==2016 || v._1._2()==2017 || v._1._2()==2018);
    
        JavaPairRDD<Tuple2<String, Integer>, Tuple3<String, Double, Integer>> dateCloseByTickerYear = completeStocksByTickerYear
                .mapValues(cssy -> new Tuple3<>(cssy.getName(), cssy.getClose(), cssy.getDay()));

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Tuple3<String, Double, Integer>, Tuple3<String, Double, Integer>>>  minMaxDateCloseByTickerYear = 
            dateCloseByTickerYear.mapValues(v -> new Tuple2<>(v, v))
            .reduceByKey((v1, v2) -> new Tuple2<>(minDateClose3(v1._1(), v2._2()), maxDateClose3(v1._1(), v2._2())));

        JavaPairRDD<Tuple2<String, Integer>, Integer> quotationChanges = minMaxDateCloseByTickerYear
            .mapValues(v -> Integer.valueOf(round(quotationChange(v))))
                .sortByKey(new SerializableComparator<Tuple2<String, Integer>>() {
                    public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
                        int cmp = t2._1().compareTo(t1._1());
                        if(cmp !=  0) return cmp;
                        return t2._2().compareTo(t1._2());
                    }
                },
                false);

        // Ticker-andamento    
        JavaPairRDD<String, Tuple2<String, StockName>> qc = quotationChanges
            .mapToPair(v -> new Tuple2<>(v._1._1(), v._1._2() + ":" + v._2() + "%"))
            .reduceByKey((v1, v2) -> v1 + "," + v2)
            .join(stockNamesByTicker);

        JavaPairRDD<String, String> result = qc
            .mapToPair(v -> new Tuple2<>(v._2._1(), v._2._2().getName()))
            .reduceByKey((v1, v2) -> v1 + "," + v2)
            .mapToPair(v -> new Tuple2<>(v._2(), v._1()));

        result.coalesce(1).saveAsTextFile(args[2]);

        spark.stop();

    }

    public static Tuple3<String, Double, Integer> maxDateClose3(Tuple3<String, Double, Integer> t1, Tuple3<String, Double, Integer> t2) {
        return t1._3() > t2._3() ? t1 : t2;
    }

    public static Tuple3<String, Double, Integer> minDateClose3(Tuple3<String, Double, Integer> t1, Tuple3<String, Double, Integer> t2) {
        return t1._3() < t2._3() ? t1 : t2;
    }

    public static Double quotationChange(Tuple2<Tuple3<String, Double, Integer>, Tuple3<String, Double, Integer>> t) {
        double firstClose = t._1._2();
        double lastClose = t._2._2();
        double quotationChangeDouble = (lastClose - firstClose) / firstClose * 100;
        return quotationChangeDouble;
    }

    private static int round(double n) {
        n += 0.5;
        n = Math.floor(n);
        return (int) n;
    }
    
}