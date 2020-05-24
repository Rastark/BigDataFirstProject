package spark.dataframe;

import java.io.Serializable;

public class StockPrice implements Serializable {

	private String ticker;
	private double close;
	private double lowThe;
	private double highThe;
	private long volume;
	private String date;
	private String[] arrayDate;

	public StockPrice() {
		ticker = "";
	}

	public StockPrice(String ticker, double close, double lowThe, double highThe, long volume, String date) {
		this.ticker = ticker;
		this.close = close;
		this.lowThe = lowThe;
		this.highThe = highThe;
		this.volume = volume;
		this.date = date;
		this.arrayDate = this.date.split("-");
  }

	public String getTicker() {
		return ticker;
	}

	public void setTicker(String ticker) {
		this.ticker = ticker;
	}

	public double getClose() {
		return close;
	}

	public void setClose(double close) {
		this.close = close;
	}

	public double getLowThe() {
		return lowThe;
	}

	public void setLowThe(double lowThe) {
		this.lowThe = lowThe;
	}

	public double getHighThe() {
		return highThe;
	}

	public void setHighThe(double highThe) {
		this.highThe = highThe;
	}

	public long getVolume() {
		return volume;
	}

	public void setVolume(long volume) {
		this.volume = volume;
	}

	public String getDate() {
		return date;
	}

    public void setDate(String date) {
        this.date = date;
        this.arrayDate = this.date.split("-");
    }

    public Integer getYear() {
        return Integer.parseInt(this.date.split("-")[0]);
    }
    
    public Integer getDay() { 
        return Integer.parseInt(this.arrayDate[1])*100 + Integer.parseInt(this.arrayDate[2]);
	}
	
	@Override
	public String toString() {
		return this.getTicker() + "," + this.getClose() + "," + this.getLowThe() + "," + this.getHighThe() + ","
				+ this.getVolume() + "," + this.getDate();
	}

}