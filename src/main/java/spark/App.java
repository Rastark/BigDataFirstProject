package spark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import spark.dataframe.StockData;

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

        SparkSession spark = SparkSession.builder().appName("FirstJob").getOrCreate();

        JavaRDD<String> lines = spark
                .read()
                .textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines
            .flatMap(s -> Arrays
            .asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, StockData> max = ones.reduceByKey((i1, i2) -> tickerComputate(i1, i2));

        JavaPairRDD<String, Integer> counts = ones
            .reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts
            .collect();

        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        // Spark Session termination

        spark.stop();
    }

    public static StockData generateStockData(StockData a, StockData b) throws ParseException {
		SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
        
        StockData stockData = new StockData();
        
		Date endDateA = sdformat.parse(a.getEndDate());
		Date endDateB = sdformat.parse(b.getEndDate());
		String endDate;
		Double endPrice;
		
		if (endDateA.compareTo(endDateB)>=0){//dateA occurs after dateB 
			endDate = a.getEndDate();
			endPrice = a.getEndPrice();
		} else {//dateB occurs after dateA 
			endDate = b.getEndDate();
			endPrice = b.getEndPrice();
		}
		
		Date startDateA = sdformat.parse(a.getStartDate());
		Date startDateB = sdformat.parse(b.getStartDate());
		String startDate;
		Double startPrice;
		
		if (startDateA.compareTo(startDateB)<=0){
			startDate = a.getStartDate();
			startPrice = a.getStartPrice();
		} else {
			startDate = b.getStartDate();
			startPrice = b.getStartPrice();
		}
		
		double max = Math.max(a.getMaxPrice(), b.getMaxPrice());
		
		double min = Math.min(a.getMinPrice(), b.getMinPrice());
		
		double totalVolume = a.getTotalVolume() + b.getTotalVolume();
		
		double stocksCount = a.getStocksCount() + b.getStocksCount();
        
        stockData.setStartDate(startDate);
        stockData.setEndDate(endDate);
        
		return new StockData(startDate,endDate,startPrice,endPrice,min,max,totalVolume,stocksCount);
	}

}