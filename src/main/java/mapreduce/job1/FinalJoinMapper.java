package mapreduce.job1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalJoinMapper extends Mapper<Object, Text, Text, Text> {

    private Text ticker = new Text();
    private Text val = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        ticker.set(fields[0]);
        val.set(fields[1]);

        context.write(ticker, val);
    }
}