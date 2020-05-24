package bigdata.job1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinPriceReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private Text minPrice = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        
        double min = Double.MAX_VALUE;
        for (DoubleWritable value : values) {
            min = Math.min(min, value.get());
        }
        minPrice.set("min:" + min);
        context.write(key, minPrice);
    }
}