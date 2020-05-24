package mapreduce.job1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChangePercentageReducer
        extends Reducer<Text, DatePrice, Text, Text> {

    private Text change = new Text();

    @Override
    protected void reduce(Text key, Iterable<DatePrice> values, Context context)
            throws IOException, InterruptedException {
        Text initialDate = new Text();
        Text finalDate = new Text();
        double initialPrice = 0;
        double finalPrice = 0;
        
        for (DatePrice val : values) {
            Text currDate = new Text(val.getDate());
            double currPrice = val.getPrice().get();
            if (initialDate.getLength() == 0 || finalDate.getLength() == 0) {
                initialDate.set(currDate);
                finalDate.set(currDate);
                initialPrice = currPrice;
                finalPrice = currPrice;
            } else {
                if (initialDate.compareTo(currDate) > 0) {
                    initialDate.set(currDate);
                    initialPrice = currPrice;
                }
                if (finalDate.compareTo(currDate) < 0) {
                    finalDate.set(currDate);
                    finalPrice = currPrice;
                }
            }
        }
        double changePerc = (finalPrice - initialPrice) / initialPrice * 100;
        changePerc = round(changePerc, 2);  // round up to the second decimal place

        StringBuilder s = new StringBuilder();
        if (changePerc > 0)
            s.append('+');
        s.append(changePerc);
        s.append("%");

        change.set("change:" + s.toString());
        context.write(key, change);
    }

    /**
     * Round up or down the number n to d decimal places.
     * @param n the number to round
     * @param d number of decimal places to round the number n to
     * @return the number n rounded up or down to d decimal places.
     */
    private double round(double n, int d) {
        if (d < 0)
            return n;
        if (d != 0)
            n = n * 10*d;
        n += 0.5;
        n = Math.floor(n);
        
        return (d == 0) ? n : n / (10*d);
    }

}