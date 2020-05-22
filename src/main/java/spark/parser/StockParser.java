package spark.parser;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

import spark.dataframe.StockPrice;

public class StockParser {

    public static StockPrice parseFileLineToStockPrice(String fileLine) throws IOException {

        StockPrice stockPrice = new StockPrice();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        // Reader reader = Files.newBufferedReader(Paths.get(""));
        // CSVReader csvReader = new CSVReader(reader);
        final CSVParserBuilder csvParserBuilder = new CSVParserBuilder().withSeparator(',').withQuoteChar('"');
        CSVParser csvParser = csvParserBuilder.build();

        try {
            String[] nextRecord;
            if ((nextRecord = csvParser.parseLine(fileLine)) != null) {
                stockPrice.setTicker(nextRecord[0]);
                stockPrice.setClose(Integer.parseInt(nextRecord[1]));
                stockPrice.setLowThe(Integer.parseInt(nextRecord[2]));
                stockPrice.setHighThe(Integer.parseInt(nextRecord[3]));
                stockPrice.setVolume(Long.parseLong(nextRecord[4]));
                stockPrice.setDate(dateFormat.parse(nextRecord[5])); // Controllare se la data è sempre presente!
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Some other error");
        }
        return stockPrice;
    }

}