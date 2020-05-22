package bigdata.job1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MeanVolumeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String[] fields = value.toString().split("\t");

        String ticker = fields[HspFields.TICKER];
        long volume = Long.parseLong(fields[HspFields.VOLUME]);
        
        context.write(new Text(ticker), new LongWritable(volume));
    }
}