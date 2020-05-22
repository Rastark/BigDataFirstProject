package mapreduce.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.finput.HsHspJoinFields;
import mapreduce.objects.HsHspJoinWritable;
import mapreduce.objects.StringBigram;

public class SectorYearTrendReducer extends Reducer<StringBigram, HsHspJoinFields, Text, Text> {

    public void reduce(Text key, Iterable<HsHspJoinWritable> values, Context context)
            throws IOException, InterruptedException {

        // Mean volume variables
        double sumVolume = 0;
        int countVolume = 0;

        // Quotation change variables
        int firstClose = 0;
        int lastClose = 0;
        int firstDay = 0;
        int lastDay = 0;

        for (HsHspJoinWritable val : values) {

            // Mean volume parameters
            sumVolume += val.getVolume().get();
            countVolume++;

        }

        double meanVolume = sumVolume / countVolume;

    }
}