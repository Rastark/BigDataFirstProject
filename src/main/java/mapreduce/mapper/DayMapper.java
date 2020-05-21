package mapreduce.mapper;

import mapreduce.finput.HsHspJoinFields;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DayMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text txtMapOutputKey = new Text("");
    private final Text txtMapOutputValue = new Text("");

    @Override
    protected void map(final LongWritable key, final Text value, final Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");

        // Sets the day set as key
        String dates = fields[HsHspJoinFields.DATE];
        String days = dates.substring(4, dates.length() - 1);
        txtMapOutputKey.set(days);

        txtMapOutputValue.set(fields[HsHspJoinFields.TICKER] + "," + fields[HsHspJoinFields.NAME] + ","
                + fields[HsHspJoinFields.CLOSE] + "," + fields[HsHspJoinFields.VOLUME] + ","
                + fields[HsHspJoinFields.DATE]);

        context.write(txtMapOutputKey, txtMapOutputValue);
    }
}