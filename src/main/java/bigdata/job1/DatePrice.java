package bigdata.job1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DatePrice implements WritableComparable<DatePrice> {

    private Text date;
    private DoubleWritable price;

    public DatePrice() {
        this.date = new Text();
        this.price = new DoubleWritable();
    }

    public DatePrice(Text date, DoubleWritable price) {
        this.date = new Text(date);
        this.price = new DoubleWritable(price.get());
    }

    public DatePrice(String date, Double price) {
        this.date = new Text(date);
        this.price = new DoubleWritable(price);
    }

    public Text getDate() {
        return date;
    }

    public DoubleWritable getPrice() {
        return price;
    }

    public void set(Text date, DoubleWritable price) {
        this.date = date;
        this.price = price;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        date.readFields(in);
        price.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        date.write(out);
        price.write(out);
    }

    @Override
    public int hashCode() {
        return date.hashCode() + price.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DatePrice) {
            DatePrice dateprice = (DatePrice)obj;
            return date.equals(dateprice.getDate())
                && price.equals(dateprice.getPrice());
        }
        return false;
    }

    @Override
    public int compareTo(DatePrice o) {
        int cmp = date.compareTo(o.getDate());
        if (cmp != 0) {
            return cmp;
        }
        return price.compareTo(o.getPrice());
    }

}