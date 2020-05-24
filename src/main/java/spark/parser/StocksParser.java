package spark.parser;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Iterator;

// import com.opencsv.CSVParser;
// import com.opencsv.CSVParserBuilder;
import com.univocity.parsers.csv.CsvFormat;

import org.apache.spark.api.java.JavaRDD;

import spark.dataframe.StockPrice;
import spark.dataframe.StockName;

public class StocksParser {

    public static JavaRDD<StockPrice> parseFileLineToStockPrice(JavaRDD<String> fileLines) throws IOException {

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            if ((fileLines) != null) {
                JavaRDD<String> fileRDD = fileLines.flatMap(fl -> Arrays.asList(fl.split(" ")).iterator());
                JavaRDD<StockPrice> fileLineRDD = fileRDD.map(fl -> new StockPrice(fl.split("\t")[0], 
                    Double.parseDouble(fl.split("\t")[1]), 
                    Double.parseDouble(fl.split("\t")[2]),
                    Double.parseDouble(fl.split("\t")[3]),
                    Long.parseLong(fl.split("\t")[4]),
                    dateFormat.parse(fl.split("\t")[5])));
                    return fileLineRDD;
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Some other error");
        }
        return null;
    }

    public static JavaRDD<StockName> parseFileLineToStock(JavaRDD<String> fileLines) throws IOException {

        try {
            if ((fileLines) != null) {
                JavaRDD<String> fileRDD = fileLines.flatMap(fl -> Arrays.asList(fl.split(" ")).iterator());
                JavaRDD<StockName> fileLineRDD = fileRDD.map(fl -> new StockName(fl.split("\t")[0],
                    fl.split("\t")[1], 
                    fl.split("\t")[2]));
                    return fileLineRDD;
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Some other error");
        }
        return null;
    }

}