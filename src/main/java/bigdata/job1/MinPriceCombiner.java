package bigdata.job1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinPriceCombiner 
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

            private DoubleWritable minPrice = new DoubleWritable();

            @Override
            protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                    throws IOException, InterruptedException {
                
                double min = Double.MAX_VALUE;
                for (DoubleWritable value : values) {
                    min = Math.min(min, value.get());
                }
                minPrice.set(min);
                context.write(key, minPrice);
            }
    
}