package bigdata.job1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChangePercentageMapper
        extends Mapper<LongWritable, Text, Text, DatePrice> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");

        String ticker = fields[HspFields.TICKER];
        String date = fields[HspFields.DATA];
        Double price = Double.parseDouble(fields[HspFields.CLOSE]);
        
        context.write(new Text(ticker), new DatePrice(date, price));
    }
    
}