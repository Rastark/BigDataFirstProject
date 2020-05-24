package mapreduce.job3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer3 extends Reducer<Text, Text, Text, Text> {

    private Text outputkey = new Text();
    private Text outputvalue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        /* key format: 'y:c,y:c,y:c',
         * values format: 'name' */

        StringBuilder names = new StringBuilder();
        names.append("{");

        for (Text val : values) {
        if (names.length() > 1)
            names.append(",");
        names.append(val.toString());
        }
        names.append("}");

        outputkey.set(names.toString());
        outputvalue.set(key);
        context.write(outputkey, outputvalue);
    }
    
}