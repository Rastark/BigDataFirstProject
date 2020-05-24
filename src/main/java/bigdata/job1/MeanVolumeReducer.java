package bigdata.job1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MeanVolumeReducer extends Reducer<Text, LongWritable, Text, Text> {

    private Text meanVol = new Text();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        
        double sum = 0;
        int count = 0;
        for (LongWritable value : values) {
            sum += value.get();
            count++;
        }
        double mean = sum / count;

        meanVol.set("volume:" + mean);
        context.write(key, meanVol);
    }
  
}