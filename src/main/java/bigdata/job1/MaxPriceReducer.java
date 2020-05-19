package bigdata.job1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxPriceReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private Text maxPrice = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        
        double max = 0;
        for (DoubleWritable value : values) {
            max = Math.max(max, value.get());
        }
        maxPrice.set("max:" + max);
        context.write(key, maxPrice);
    }
}