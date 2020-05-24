package mapreduce.job2.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StringBigram implements WritableComparable<StringBigram> {

    private Text firstKey;
    private Text secondKey;

    public StringBigram(Text firstKey, Text secondKey) {
        this.firstKey = firstKey;
        this.secondKey = secondKey;
    }

    public StringBigram(String firstKey, String secondKey) {
        this.firstKey = new Text(firstKey);
        this.secondKey = new Text(secondKey);
    }

    public StringBigram() {
        this.firstKey = new Text();
        this.secondKey = new Text();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        firstKey.write(out);
        secondKey.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstKey.readFields(in);
        secondKey.readFields(in);
    }

    @Override
    public int compareTo(StringBigram o) {
        int cmp = firstKey.compareTo(o.firstKey);
        if (cmp != 0)
            return cmp;
        return secondKey.compareTo(o.secondKey);
    }

    public int hashCode() {
        return firstKey.hashCode() + secondKey.hashCode();
    }

    public boolean equals(Object o) {
        if (o instanceof StringBigram) {
            StringBigram sb = (StringBigram) o;
            return firstKey.equals(sb.firstKey) && secondKey.equals(sb.secondKey);
        }
        return false;
    }

    /**
     * @return the firstKey
     */
    public Text getFirstKey() {
        return firstKey;
    }

    /**
     * @return the secondKey
     */
    public Text getSecondKey() {
        return secondKey;
    }

    /**
     * @param firstKey the firstKey to set
     */
    public void setFirstKey(Text firstKey) {
        this.firstKey = firstKey;
    }

    /**
     * @param secondKey the secondKey to set
     */
    public void setSecondKey(Text secondKey) {
        this.secondKey = secondKey;
    }

    @Override
    public String toString() {
        return "[" + firstKey + ", " + secondKey + "]";
    }

    
}