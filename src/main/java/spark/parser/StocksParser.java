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

import org.apache.commons.csv.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple4;
import spark.dataframe.StockPrice;

public class StocksParser {

    public static JavaRDD<StockPrice> parseFileLineToStockPrice(JavaRDD<String> fileLines) throws IOException {

        // StockPrice stockPrice = new StockPrice();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        // Reader reader = Files.newBufferedReader(Paths.get(""));
        // CSVReader csvReader = new CSVReader(reader);
        
        // CSVParserBuilder csvParserBuilder = new CSVParserBuilder().withSeparator('\t').withQuoteChar('"');
        // CSVParser csvParser = csvParserBuilder.build(); 

        // CSVParser csvParser = new CSVParser(reader, CSVFormat.newFormat('\t'));

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
                // stockPrice.setTicker(fl.split("\t")[0]);
                // stockPrice.setClose(Double.parseDouble(fl.split("\t")[1]));
                // stockPrice.setLowThe(Double.parseDouble(fl.split("\t")[2]));
                // stockPrice.setHighThe(Double.parseDouble(fl.split("\t")[3]));
                // stockPrice.setVolume(Long.parseLong(fl.split("\t")[4]));
                // stockPrice.setDate(dateFormat.parse(fl.split("\t")[5])); // Controllare se la data è sempre presente!
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Some other error");
        }
        return null;
    }

}