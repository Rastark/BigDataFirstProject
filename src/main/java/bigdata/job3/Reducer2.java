package bigdata.job3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<Text, Text, Text, Text> {

    private Text outputvalue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        /* key is in the form 'name:ticker',
         * values are in the form 'year:change' */

        List<Text> trends = new ArrayList<>();
        for (Text val : values) {
            trends.add(val);
        }

        trends = sortTrends(trends).subList(0, 3);
        String formattedTrends = listToString(trends);
        
        outputvalue.set(formattedTrends);
        // output format: <'name:ticker', '[year:+/-change%, ... , ...]'>
        context.write(key, outputvalue);
    }


    private static <T extends Text> List<T> sortTrends(List<T> list) {

        Collections.sort(list, new Comparator<T>() {
            @Override
            public int compare(T o1, T o2) {
                return o2.compareTo(o1);
            }
        });

        return list;
    }

    /**
     * return formatted trends list
     */
    private static String listToString(List<Text> list) {
        StringBuilder sb = new StringBuilder();

        for (Text t : list) {
            if (sb.length() != 0)
                sb.append(",");

            String[] fields = t.toString().split(":");
            String year = fields[0];
            String change = fields[1];

            if (change.charAt(0) == '-')
                sb.append(year + ":" + change + "%");
            else
                sb.append(year + ":+" + change + "%");
        }

        return sb.toString();
    }
    
}