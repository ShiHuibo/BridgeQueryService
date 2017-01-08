package cn.edu.fudan.dsm.basic.common;

/**
 * Created by huibo on 2016/12/7.
 */
public class Interval {

    private long left;

    private long right;

    private double epsilon;

    public Interval(long left, long right, double epsilon) {
        this.left = left;
        this.right = right;
        this.epsilon = epsilon;
    }

    public long getLeft() {
        return left;
    }

    public void setLeft(long left) {
        this.left = left;
    }

    public long getRight() {
        return right;
    }

    public void setRight(long right) {
        this.right = right;
    }

    public double getEpsilon() {
        return epsilon;
    }

    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    @Override
    public String toString() {
        return "[" + "" + left + ", " + right + ']';
    }

}
