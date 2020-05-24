package mapreduce.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class HsHspJoinWritable implements Writable {

    private Text ticker;
    private Text name;
    private DoubleWritable close;
    private LongWritable volume;
    private IntWritable dayInt;

    public HsHspJoinWritable() {
        ticker = new Text();
        name = new Text();
        close = new DoubleWritable();
        volume = new LongWritable();
        dayInt = new IntWritable();
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

    public DoubleWritable getClose() {
        return close;
    }

    public void setClose(DoubleWritable close) {
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

    @Override
    public String toString() {
        return "HsHspJoinWritable [close=" + close + ", dayInt=" + dayInt + ", name=" + name + ", ticker=" + ticker
                + ", volume=" + volume + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((close == null) ? 0 : close.hashCode());
        result = prime * result + ((dayInt == null) ? 0 : dayInt.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((ticker == null) ? 0 : ticker.hashCode());
        result = prime * result + ((volume == null) ? 0 : volume.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HsHspJoinWritable other = (HsHspJoinWritable) obj;
        if (close == null) {
            if (other.close != null)
                return false;
        } else if (!close.equals(other.close))
            return false;
        if (dayInt == null) {
            if (other.dayInt != null)
                return false;
        } else if (!dayInt.equals(other.dayInt))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (ticker == null) {
            if (other.ticker != null)
                return false;
        } else if (!ticker.equals(other.ticker))
            return false;
        if (volume == null) {
            if (other.volume != null)
                return false;
        } else if (!volume.equals(other.volume))
            return false;
        return true;
    }

}