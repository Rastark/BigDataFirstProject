package mapreduce.reducer;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QuotationVarReducer extends Reducer<Text, ArrayWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<ArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        for (ArrayWritable value : values) {
            value[]
        }

        context.write(key, new IntWritable());
    }
}