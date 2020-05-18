package mapreduce.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgYearlyVolReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(final Text key, final Text values, final Context context)
            throws IOException, InterruptedException {
        context.write(key, values);
    }
}