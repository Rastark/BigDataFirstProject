package bigdata.job1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinPriceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String[] fields = value.toString().split(",");

        String ticker = fields[HspFields.TICKER];
        double lowPrice = Double.parseDouble(fields[HspFields.LOW]);

        context.write(new Text(ticker), new DoubleWritable(lowPrice));
    }

}