package spark.dataframe;

import java.io.Serializable;

public class CompleteStock implements Serializable {

    private String ticker;
    private String name;
    private String sector;
    private double close;
    private long volume;
    private String date;
    private String[] arrayDate;

    public CompleteStock() {
        ticker = "";
        name = "";
        sector = "";
    }

    public CompleteStock(String ticker, String name, String sector, double close, long volume, String date) {
        this.ticker = ticker;
        this.name = name;
        this.sector = sector;
        this.close = close;
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

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
        this.arrayDate = this.date.split("-");
    }

    public Integer getYear() {
        return Integer.valueOf(this.date.split("-")[0]);
    }
    
    public Integer getDay() { 
        return Integer.valueOf(this.arrayDate[1])*100 + Integer.valueOf(this.arrayDate[2]);
    }

    @Override
    public String toString() {
        return this.getTicker() + "," + this.getName() + "," + this.getSector() + "," + this.getClose() + "," + this.getVolume() + ","
                + this.getDate();
    }

    public String getSector() {
        return sector;
    }

    public void setSector(String sector) {
        this.sector = sector;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(close);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + ((date == null) ? 0 : date.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((sector == null) ? 0 : sector.hashCode());
        result = prime * result + ((ticker == null) ? 0 : ticker.hashCode());
        result = prime * result + (int) (volume ^ (volume >>> 32));
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
        CompleteStock other = (CompleteStock) obj;
        if (Double.doubleToLongBits(close) != Double.doubleToLongBits(other.close))
            return false;
        if (date == null) {
            if (other.date != null)
                return false;
        } else if (!date.equals(other.date))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (sector == null) {
            if (other.sector != null)
                return false;
        } else if (!sector.equals(other.sector))
            return false;
        if (ticker == null) {
            if (other.ticker != null)
                return false;
        } else if (!ticker.equals(other.ticker))
            return false;
        if (volume != other.volume)
            return false;
        return true;
    }

}