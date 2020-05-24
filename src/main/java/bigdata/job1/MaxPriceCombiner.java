package bigdata.job1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxPriceCombiner 
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

            private DoubleWritable maxPrice = new DoubleWritable();

            @Override
            protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                    throws IOException, InterruptedException {
                
                double max = 0;
                for (DoubleWritable value : values) {
                    max = Math.max(max, value.get());
                }
                maxPrice.set(max);
                context.write(key, maxPrice);
            }
    
}