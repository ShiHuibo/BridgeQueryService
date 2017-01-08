package cn.edu.fudan.dsm.basic.common.entity;

/**
 * Created by huibo on 2016/12/16.
 */
public class IndexRowKey {

    private long first;
    private String channelCode;
    private String value;

    public IndexRowKey(String rowKey) {
        if (rowKey == null) return;
        String[] strings = rowKey.split("_");
        this.first = Long.parseLong(strings[0]);
        this.channelCode = strings[1];
        this.value = strings[2];
    }

    @Override
    public String toString() {
        return first + "_" + channelCode + "_" + value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        IndexRowKey that = (IndexRowKey) obj;

        return (first == that.getFirst() && channelCode.equals(that.getChannelCode()) && value.equals(that.getValue()));
    }

    public long getFirst() {
        return first;
    }

    public void setFirst(long first) {
        this.first = first;
    }

    public String getChannelCode() {
        return channelCode;
    }

    public void setChannelCode(String channelCode) {
        this.channelCode = channelCode;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
