package spark.dataframe;

import java.io.Serializable;

public class StockPrice implements Serializable {

	private String ticker;
	private double close;
	private double lowThe;
	private double highThe;
	private long volume;
	private int year;
	private int day;

	public StockPrice() {
		ticker = "";
	}

	public StockPrice(String ticker, double close, double lowThe, double highThe, long volume, int year, int day) {
		this.ticker = ticker;
		this.close = close;
		this.lowThe = lowThe;
		this.highThe = highThe;
		this.volume = volume;
		this.year = year;
		this.day = day;
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
	
	@Override
	public String toString() {
		return this.getTicker() + "," + this.getClose() + "," + this.getLowThe() + "," + this.getHighThe() + ","
				+ this.getVolume() + "," + this.getYear() + "," + this.getDay();
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(close);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + day;
		temp = Double.doubleToLongBits(highThe);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(lowThe);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((ticker == null) ? 0 : ticker.hashCode());
		result = prime * result + (int) (volume ^ (volume >>> 32));
		result = prime * result + year;
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
		StockPrice other = (StockPrice) obj;
		if (Double.doubleToLongBits(close) != Double.doubleToLongBits(other.close))
			return false;
		if (day != other.day)
			return false;
		if (Double.doubleToLongBits(highThe) != Double.doubleToLongBits(other.highThe))
			return false;
		if (Double.doubleToLongBits(lowThe) != Double.doubleToLongBits(other.lowThe))
			return false;
		if (ticker == null) {
			if (other.ticker != null)
				return false;
		} else if (!ticker.equals(other.ticker))
			return false;
		if (volume != other.volume)
			return false;
		if (year != other.year)
			return false;
		return true;
	}

}