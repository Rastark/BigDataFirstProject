package spark.dataframe;

import java.io.Serializable;
import java.util.Date;

public class CompleteStock implements Serializable {

    private String ticker;
    private String name;
    private double close;
    private long volume;
    private Date date;

    public CompleteStock() {
        ticker = "";
        name = "";
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getClose() {
        return close;
    }

    public void setClose(double close) {
        this.close = close;
    }

    public long getVolume() {
        return volume;
    }

    public void setVolume(long volume) {
        this.volume = volume;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return this.getTicker() + "," + this.getName() + "," + this.getClose() + "," + this.getVolume() + ","
                + this.getDate();
    }
}