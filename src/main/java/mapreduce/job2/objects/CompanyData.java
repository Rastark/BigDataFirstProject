package mapreduce.job2.objects;

public class CompanyData {

    private int firstDay;
    private int lastDay;
    private double firstClose;
    private double lastClose;

    public CompanyData() {
        this.firstDay = 9999;        
        this.lastDay = 0;
        this.firstClose = 0;
        this.lastClose = 0;
    }

    public CompanyData(int firstDay, int lastDay, double firstClose, double lastClose) {
        this.firstDay = firstDay;        
        this.lastDay = lastDay;
        this.firstClose = firstClose;
        this.lastClose = lastClose;
    }

    public int getFirstDay() {
        return firstDay;
    }

    public void setFirstDay(int firstDay) {
        this.firstDay = firstDay;
    }

    public int getLastDay() {
        return lastDay;
    }

    public void setLastDay(int lastDay) {
        this.lastDay = lastDay;
    }

    public double getFirstClose() {
        return firstClose;
    }

    public void setFirstClose(double firstClose) {
        this.firstClose = firstClose;
    }

    public double getLastClose() {
        return lastClose;
    }

    public void setLastClose(double lastClose) {
        this.lastClose = lastClose;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(firstClose);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + firstDay;
        temp = Double.doubleToLongBits(lastClose);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + lastDay;
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
        CompanyData other = (CompanyData) obj;
        if (Double.doubleToLongBits(firstClose) != Double.doubleToLongBits(other.firstClose))
            return false;
        if (firstDay != other.firstDay)
            return false;
        if (Double.doubleToLongBits(lastClose) != Double.doubleToLongBits(other.lastClose))
            return false;
        if (lastDay != other.lastDay)
            return false;
        return true;
    }

    
}