package bigdata.job3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object, Text, Text, Text> {

    private Text outputkey = new Text();
    private Text outputvalue = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // input value is in the form 'name:ticker  year:change'
        String[] fields = value.toString().split("\t");

        outputkey.set(fields[0]);
        outputvalue.set(fields[1]);
        context.write(outputkey, outputvalue);
        // System.out.println("<" + outputkey.toString() + "> , <" + outputvalue.toString() + ">");
    }
    
}