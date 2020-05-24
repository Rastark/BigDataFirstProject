package mapreduce.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StringTrigram implements WritableComparable<StringTrigram> {

    private Text firstKey;
    private Text secondKey;
    private Text thirdKey;

    public StringTrigram(Text firstKey, Text secondKey, Text thirdKey) {
        this.firstKey = firstKey;
        this.secondKey = secondKey;
        this.thirdKey = thirdKey;
    }

    public StringTrigram(String firstKey, String secondKey, String thirdKey) {
        this.firstKey = new Text(firstKey);
        this.secondKey = new Text(secondKey);
        this.thirdKey = new Text(thirdKey);
    }

    public StringTrigram() {
        this.firstKey = new Text();
        this.secondKey = new Text();
        this.thirdKey = new Text();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        firstKey.write(out);
        secondKey.write(out);
        thirdKey.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstKey.readFields(in);
        secondKey.readFields(in);
        thirdKey.readFields(in);
    }

    @Override
    public int compareTo(StringTrigram o) {
        int cmp = firstKey.compareTo(o.firstKey);
        if (cmp != 0)
            return cmp;

        cmp = secondKey.compareTo(o.secondKey);
        if (cmp != 0)
            return cmp;

        return thirdKey.compareTo(o.thirdKey);
    }

    public int hashCode() {
        return firstKey.hashCode() + secondKey.hashCode() + thirdKey.hashCode();
    }

    public boolean equals(Object o) {
        if (o instanceof StringTrigram) {
            StringTrigram st = (StringTrigram) o;
            return firstKey.equals(st.firstKey) && secondKey.equals(st.secondKey) && thirdKey.equals(st.thirdKey);
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
     * @return the thirdKey
     */
    public Text getThirdKey() {
        return thirdKey;
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

    /**
     * @param thirdKey the thirdKey to set
     */
    public void setThirdKey(Text thirdKey) {
        this.thirdKey = thirdKey;
    }
}