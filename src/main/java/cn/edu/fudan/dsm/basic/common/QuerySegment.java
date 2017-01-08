package cn.edu.fudan.dsm.basic.common;

/**
 * Created by huibo on 2016/12/8.
 */
public class QuerySegment {

    private double mean;

    private String meanStr;

    private String beginRound;

    private String endRound;

    private int order;

    private int count;

    public QuerySegment(double mean, String beginRound, String endRound, int order, int count) {
        this.mean = mean;
        this.meanStr = String.valueOf(Double.doubleToLongBits(mean));
        this.beginRound = beginRound;
        this.endRound = endRound;
        this.order = order;
        this.count = count;
    }

    @Override
    public String toString() {
        return String.valueOf(order);
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public String getBeginRound() {
        return beginRound;
    }

    public void setBeginRound(String beginRound) {
        this.beginRound = beginRound;
    }

    public String getEndRound() {
        return endRound;
    }

    public void setEndRound(String endRound) {
        this.endRound = endRound;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getMeanStr() {
        return meanStr;
    }

    public void setMeanStr(String meanStr) {
        this.meanStr = meanStr;
    }

}
