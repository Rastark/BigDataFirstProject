package mapreduce.mapper;

import mapreduce.finput.HsHspJoinFields;
import mapreduce.objects.HsHspJoinWritable;
import mapreduce.objects.StringBigram;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SectorYearMapper extends Mapper<LongWritable, Text, StringBigram, HsHspJoinWritable> {

    private final StringBigram outputKey = new StringBigram();
    private final HsHspJoinWritable outputValue = new HsHspJoinWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");

        String ticker = fields[HsHspJoinFields.TICKER];
        String name = fields[HsHspJoinFields.NAME];
        String sector = fields[HsHspJoinFields.SECTOR];
        double close = Double.parseDouble(fields[HsHspJoinFields.CLOSE]);
        long volume = Long.parseLong(fields[HsHspJoinFields.VOLUME]);
        String date = fields[HsHspJoinFields.DATE];

        // Key
        outputKey.setFirstKey(new Text(sector));
        String[] dateArray = date.split("-");
        String year = dateArray[0];
        outputKey.setSecondKey(new Text(year));

        int dayInt = Integer.parseInt(dateArray[1]) * 100 + Integer.parseInt(dateArray[2]);
                
        // Value
        outputValue.setTicker(new Text(ticker));
        outputValue.setName(new Text(name));
        outputValue.setClose(new DoubleWritable(close));
        outputValue.setVolume(new LongWritable(volume));
        outputValue.setDayInt(new IntWritable(dayInt));

        context.write(outputKey, outputValue);
    }
}