package mapreduce.job3;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
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

        List<Text> trends = new LinkedList<>();
        System.out.print("<" + key.toString() + "> , <");
        for (Text val : values) {
            trends.add(new Text(val));
            System.out.print(val + ", ");
        }
        System.out.println(">");

        // trends = sortTrends(trends);
        Collections.sort(trends, Collections.reverseOrder());
        
        trends = lastThreeYears(trends);
        System.out.println("<" + key.toString() + "> ==> { " + trends + " }");
        String formattedTrends = listToString(trends);
        
        outputvalue.set(formattedTrends);
        // output format: <'name:ticker', '[year:change, ... , ...]'>
        context.write(key, outputvalue);
    }


    private static List<Text> lastThreeYears(List<Text> list) {
        List<Text> last3years = new LinkedList<>();
        int lastYear = -1;
        int count = 0;

        for (Text t : list) {
            int currYear = Integer.parseInt(t.toString().split(":")[0]);
            if (lastYear < 0 || currYear == lastYear-1) {
                last3years.add(new Text(t));
                lastYear = currYear;
            } else {
                lastYear--;
                last3years.add(new Text(lastYear + ":0"));
            }
            count++;
            if (count == 3)
                break;
        }
        while (count < 3) {
            lastYear--;
            last3years.add(new Text(lastYear + ":0"));
            count++;
        }
        return last3years;
    }

    // private static <T extends Text> List<T> sortTrends(List<T> list) {
    //     List<T> newList = new LinkedList<>(list);
    //     Collections.sort(newList, Collections.reverseOrder());
    //     return newList;
    // }

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