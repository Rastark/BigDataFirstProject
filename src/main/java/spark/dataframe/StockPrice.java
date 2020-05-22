package spark.dataframe;

import java.io.Serializable;
import java.util.Date;

public class StockPrice implements Serializable {

  private String ticker;
  private int close;
  private int lowThe;
  private int highThe;
  private long volume;
  private Date date;

  public StockPrice() {
    ticker = "";
  }

  public String getTicker() {
    return ticker;
  }

  public void setTicker(String ticker) {
    this.ticker = ticker;
  }

  public int getClose() {
    return close;
  }

  public void setClose(int close) {
    this.close = close;
  }

  public int getLowThe() {
    return lowThe;
  }

  public void setLowThe(int lowThe) {
    this.lowThe = lowThe;
  }

  public int getHighThe() {
    return highThe;
  }

  public void setHighThe(int highThe) {
    this.highThe = highThe;
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
    return this.getTicker() + "," + this.getClose() + "," + this.getLowThe() + "," + this.getHighThe() + ","
        + this.getVolume() + "," + this.getDate();
  }

}