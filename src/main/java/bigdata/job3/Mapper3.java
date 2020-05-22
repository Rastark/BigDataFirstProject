package bigdata.job3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper3 extends Mapper<Object, Text, Text, Text> {

    private Text outputkey = new Text();
    private Text outputvalue = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        /* input format: <'name:ticker', '[year:+/-change%, ... , ...]'> */

        String[] fields = value.toString().split("\t");
        String name = fields[0].split(":")[0];
        String trends = fields[1];

        outputkey.set(trends);
        outputvalue.set(name);
        // output format: <'y:c,y:c,y:c', 'name'>
        context.write(outputkey, outputvalue);
    }
    
}