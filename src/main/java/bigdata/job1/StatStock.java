package bigdata.job1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StatStock implements WritableComparable<StatStock> {

    private Text change;
    private Text min;
    private Text max;
    private Text volume;

    public StatStock() {
        this.change = new Text();
        this.min = new Text();
        this.max = new Text();
        this.volume = new Text();
    }

    public StatStock(Text change, Text min, Text max, Text volume) {
        this.change = new Text(change);
        this.min = new Text(min);
        this.max = new Text(max);
        this.volume = new Text(volume);
    }

    public Text getChange() {
        return change;
    }

    public Text getMin() {
        return min;
    }

    public Text getMax() {
        return max;
    }

    public Text getVolume() {
        return volume;
    }

    public void setChange(Text change) {
        this.change = change;
    }

    public void setMin(Text min) {
        this.min = min;
    }

    public void setMax(Text max) {
        this.max = max;
    }

    public void setVolume(Text volume) {
        this.volume = volume;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        change.readFields(in);
        min.readFields(in);
        max.readFields(in);
        volume.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        change.write(out);
        min.write(out);
        max.write(out);
        volume.write(out);
    }

    @Override
    public int compareTo(StatStock obj) {
        String ch = change.toString();
        String o = obj.getChange().toString();

        Double valCh = Double.valueOf(ch.substring(0, ch.length()-1));
        Double valO = Double.valueOf(o.substring(0, o.length()-1));
        return valCh.compareTo(valO);
    }

    @Override
    public String toString() {
        return change.toString() + ","
             + min.toString() + ","
             + max.toString() + ","
             + volume.toString();
    }
    
}