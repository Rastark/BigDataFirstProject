package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import spark.dataframe.StockPrice;
import spark.parser.StockParser;

public class StockMining {

    private String pathToFile;

    public StockMining(String file) {
        this.pathToFile = file;
    }

    public JavaRDD<StockPrice> loadData() {

        SparkConf conf = new SparkConf().setAppName("Stock mining");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<StockPrice> stockprices = sc.textFile(pathToFile)
                .map(line -> StockParser.parseFileLineToStockPrice(line));
        return stockprices;
    }

    public JavaRDD<StockPrice> filterByYears() {

    }

}