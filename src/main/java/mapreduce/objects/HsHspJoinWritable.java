package mapreduce.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class HsHspJoinWritable implements Writable {

    private Text ticker;
    private Text name;
    private IntWritable close;
    private LongWritable volume;
    private IntWritable dayInt;

    public HsHspJoinWritable() {
        ticker = new Text();
        name = new Text();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ticker.write(out);
        name.write(out);
        close.write(out);
        volume.write(out);
        dayInt.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ticker.readFields(in);
        name.readFields(in);
        close.readFields(in);
        volume.readFields(in);
        dayInt.readFields(in);

    }

    public Text getTicker() {
        return ticker;
    }

    public void setTicker(Text ticker) {
        this.ticker = ticker;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public IntWritable getClose() {
        return close;
    }

    public void setClose(IntWritable close) {
        this.close = close;
    }

    public LongWritable getVolume() {
        return volume;
    }

    public void setVolume(LongWritable volume) {
        this.volume = volume;
    }

    public IntWritable getDayInt() {
        return dayInt;
    }

    public void setDayInt(IntWritable dayInt) {
        this.dayInt = dayInt;
    }

}