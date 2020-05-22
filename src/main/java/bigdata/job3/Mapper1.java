package bigdata.job3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<Object, Text, Text, Text> {

    private Text outputkey = new Text();
    private Text outputvalue = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");
        String ticker = fields[HsHspJoinFields.TICKER];
        String name = fields[HsHspJoinFields.NAME];
        String close = fields[HsHspJoinFields.CLOSE];
        String date[] = fields[HsHspJoinFields.DATE].split("-");
        String year = date[0];
        int dateInt = monthDayToInt(date[1], date[2]);

        outputkey.set(name + ":" + ticker + ":" + year);
        outputvalue.set(dateInt + ":" + close);

        context.write(outputkey, outputvalue);
    }

    private static int monthDayToInt(String month, String day) {
        int m = Integer.parseInt(month);
        int d = Integer.parseInt(day);
        return m * 100 + d;
    }
    
}