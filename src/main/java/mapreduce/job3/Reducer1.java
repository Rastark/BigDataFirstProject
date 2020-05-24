package mapreduce.job3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {

    private Text outputkey = new Text();
    private Text outputvalue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        /* key is in the form 'name:ticker:year',
         * values are in the form 'dateInt:close' */
        
        String[] keyFields = key.toString().split(":");
        String name = keyFields[0];
        String ticker = keyFields[1];
        String year = keyFields[2];

        int firstDay = 9999;
        int lastDay = 0;
        double firstPrice = 0;
        double lastPrice = 0;

        for (Text val : values) {
            String[] valueFields = val.toString().split(":");
            int currDay = Integer.parseInt(valueFields[0]);
            double currPrice = Double.parseDouble(valueFields[1]);
            if (currDay < firstDay) {
                firstDay = currDay;
                firstPrice = currPrice;
            }
            if (currDay > lastDay) {
                lastDay = currDay;
                lastPrice = currPrice;
            }
        }
        double change = (lastPrice - firstPrice) / firstPrice * 100;
        change = round(change, 0);
        int changeInt = (int) change;
        
        outputkey.set(name + ":" + ticker);
        outputvalue.set(year + ":" + changeInt);
        context.write(outputkey, outputvalue);
    }

    /**
     * Round up or down the number n to d decimal places.
     * @param n the number to round
     * @param d number of decimal places to round the number n to
     * @return the number n rounded up or down to d decimal places.
     */
    private static double round(double n, int d) {
        if (d < 0)
            return n;
        if (d != 0)
            n = n * 10*d;
        n += 0.5;
        n = Math.floor(n);

        return (d == 0) ? n : n / (10*d);
    }
    
}