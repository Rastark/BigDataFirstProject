package spark;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import spark.dataframe.*;
import spark.utils.SerializableComparator;
import spark.utils.StocksParser;

public class Job2 {

    public static void main(final String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: <commandName> <file>");
            System.exit(1);
        }

        // Spark session creation
        // SparkConf conf = new SparkConf()
        // .set("spark.master", "local")
        // .set("spark.ui.port", "8040")
        // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        // .set("spark.kryo.registrator", "spark.dataframe.CompleteStock")
        // .set("spark.kryo.registrator", "spark.dataframe.StockName")
        // .set("spark.kryo.registrator", "spark.dataframe.StockPrice");

        // JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

        // String path= "<dataset path>/employee";
        
        // SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Test");
        SparkSession spark = SparkSession.builder().appName("BigDataProjectJob2").getOrCreate();

        // Import and Map creation
        JavaRDD<String> priceLines = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<StockPrice> stockPrices = StocksParser.parseFileLineToStockPrice(priceLines);

        JavaRDD<String> stockLines = spark.read().textFile(args[1]).javaRDD();
        JavaRDD<StockName> stockNames = StocksParser.parseFileLineToStock(stockLines);

        // Join
        JavaPairRDD<String, StockName> stockNamesByTicker = stockNames.keyBy((StockName::getTicker))
            .persist(StorageLevel.MEMORY_ONLY_SER());
        JavaPairRDD<String, StockPrice> stockPricesByTicker = stockPrices.keyBy((StockPrice::getTicker));

        JavaPairRDD<String, CompleteStock> completeStocksByTicker = stockNamesByTicker.join(stockPricesByTicker)
            .mapValues(snp -> new CompleteStock(
                snp._1().getTicker(), 
                snp._1().getName(), 
                snp._1().getSector(),
                snp._2().getClose(), 
                snp._2().getVolume(), 
                snp._2().getYear(), 
                snp._2().getDay()));

        // JavaPairRDD<String, CompleteStock> completeStocksByTicker = stockNamePrices

        JavaPairRDD<Tuple2<String, Integer>, CompleteStock> completeStocksBySectorYear = completeStocksByTicker
            .mapToPair(cst -> new Tuple2<>(new Tuple2<>(cst._2().getSector(), cst._2().getYear()), cst._2()));

        // Job2a
        JavaPairRDD<Tuple2<String, Integer>, Double> meanSectorDateVolumes = completeStocksBySectorYear
            .mapValues(v -> new Tuple2<>(v.getVolume(), Long.valueOf(1)))
            .reduceByKey((v1, v2) -> new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2()))
            .mapValues(v -> Double.valueOf(v._1()) / v._2());

        // JavaPairRDD<Tuple2<String, Integer>, Double> meanSectorDateVolumes =
        // volumesBySectorYear

        // JavaPairRDD<Tuple2<String, Integer>, Double> meanSectorDateVolumes =
        // totalSectorDateVolumes

        // Jon2b
        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> dateCloseBySectorYear = completeStocksBySectorYear
            .mapValues(cssy -> new Tuple2<>(cssy.getClose(), cssy.getDay()));

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>>> firstLastCloseBySectorYear = dateCloseBySectorYear
            .mapValues(v -> new Tuple2<>(v, v))
            .reduceByKey((v1, v2) -> new Tuple2<>(minDateClose(v1._1(), v2._1()), maxDateClose(v1._2(), v2._2())));

        // JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> minDateCloseBySectorYear = dateCloseBySectorYear
        //     .reduceByKey((v1, v2) -> minDateClose(v1, v2));

        // JavaPairRDD<Tuple2<String, Integer>, Tuple2<Double, Integer>> maxDateCloseBySectorYear = dateCloseBySectorYear
        //     .reduceByKey((v1, v2) -> maxDateClose(v1, v2));

        // JavaPairRDD<Tuple2<String, Integer>, Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>>> joinDateCloseBySectorYear = minDateCloseBySectorYear
        //     .join(maxDateCloseBySectorYear);

        JavaPairRDD<Tuple2<String, Integer>, Double> meanQuotationChanges = firstLastCloseBySectorYear
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

        JavaPairRDD<Tuple2<String, Integer>, Tuple2<Tuple2<Double, Double>, Double>> orderedResult = result
            .sortByKey(new SerializableComparator<Tuple2<String, Integer>>(){
                public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
                    int cmp = t1._1().compareTo(t2._1());
                    if(cmp !=  0) return cmp;
                    return t1._2().compareTo(t2._2());
                }
            });

        orderedResult.coalesce(1).saveAsTextFile(args[2]);

        spark.stop();

    }

    // Spark Session termination
    public static Tuple2<Double, Integer> maxDateClose(Tuple2<Double, Integer> t1, Tuple2<Double, Integer> t2) {
        return t1._2() > t2._2() ? t1 : t2;
    }

    public static Tuple2<Double, Integer> minDateClose(Tuple2<Double, Integer> t1, Tuple2<Double, Integer> t2) {
        return t1._2() < t2._2() ? t1 : t2;
    }

    public static Double quotationChange(Tuple2<Tuple2<Double, Integer>, Tuple2<Double, Integer>> t) {
        double firstClose = t._1._1();
        double lastClose = t._2._1();
        double quotationChangeDouble = (lastClose - firstClose) / firstClose * 100;
        return quotationChangeDouble;
    }

}