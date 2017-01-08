package cn.edu.fudan.dsm.basic.common.entity;

import cn.edu.fudan.dsm.basic.common.util.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by huibo on 2016/12/8.
 */
public class TimeSeriesRowKey {

    private String channelCode;
    private long first;

    public TimeSeriesRowKey(String channelCode, long first) {
        this.channelCode = channelCode;
        this.first = first;
    }

    public TimeSeriesRowKey(String rowKey) {
        if (rowKey == null) return;
        String[] strings = rowKey.split("_");
        this.channelCode = strings[0];
        this.first = Long.parseLong(strings[1]);
    }

    public TimeSeriesRowKey(byte[] rowKey) {
        if (rowKey == null || rowKey.length == 0) return;
        String[] strings = Bytes.toString(rowKey).split("_");
        this.channelCode = strings[0];
        this.first = Long.parseLong(strings[1]);
    }

    @Override
    public String toString() {
        return channelCode + "_" + StringUtils.toStringFixedWidth(first, 13);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeSeriesRowKey that = (TimeSeriesRowKey) o;

        return (first == that.first && channelCode.equals(that.channelCode));
    }

//    @Override
//    public int hashCode() {
//        return (int) (first ^ (first >>> 32));
//    }

    public long getFirst() {
        return first;
    }

    public String getChannelCode() {
        return channelCode;
    }

}
