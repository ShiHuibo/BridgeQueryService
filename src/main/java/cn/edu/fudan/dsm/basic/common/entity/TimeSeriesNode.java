package cn.edu.fudan.dsm.basic.common.entity;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by huibo on 2016/12/7.
 * <p>
 * a node of the TimeSeries
 * </p>
 * Properties: data(timestamp, value)
 */
public class TimeSeriesNode {

    public static int TIME_STEP = 1000; //ms
    private long offset;
    private List<Pair<Long, Float>> data;

    public TimeSeriesNode() {
        this.data = new ArrayList<>();
        this.offset = 0;
    }

    public void parseBytes(long offset, byte[] inputConcatData) {
        this.offset = offset;
        byte[] concatData = Base64.decode(new String(inputConcatData));
        int nodeSize = Bytes.SIZEOF_LONG + Bytes.SIZEOF_FLOAT;
        Pair<Long, Float> preItem = new Pair<>();
        for (int i = 0; i < concatData.length / nodeSize; i++) {
            byte[] tmp = new byte[Bytes.SIZEOF_LONG];
            System.arraycopy(concatData, nodeSize * i, tmp, 0, Bytes.SIZEOF_LONG);
            long tmpTimestamp = Bytes.toLong(tmp) / TIME_STEP;
            tmp = new byte[Bytes.SIZEOF_FLOAT];
            System.arraycopy(concatData, nodeSize * i + Bytes.SIZEOF_LONG, tmp, 0, Bytes.SIZEOF_FLOAT);
            float tmpValue = Bytes.toFloat(tmp);
            if (i > 0) {
                long ts = preItem.getFirst();
                float tv = preItem.getSecond();
                float vStep = (tmpValue - tv) / (tmpTimestamp - ts);
                ts += 1;
                tv += vStep;
                while (tmpTimestamp > ts) {
                    data.add(new Pair<>(ts, tv));
                    ts++;
                    tv += vStep;
                }
            }
            preItem = new Pair<>(tmpTimestamp, tmpValue);
            data.add(preItem);
        }
    }

    @Override
    public String toString() {
        return "TimeSeriesNode{ data=" + data + '}';
    }

    public long getOffset() {
        return offset;
    }

    public List<Pair<Long, Float>> getData() {
        return data;
    }

    // only return values
    public List<Float> getData(Pair<Long, Float> lastNode, long lastOffset) {
        List<Float> values = new ArrayList<>();
        if (data.size() > 0) {
            if (lastNode != null && lastNode.getFirst() != null && lastNode.getSecond() != null && lastOffset > 0) {
                long ts = lastNode.getFirst() + lastOffset / TIME_STEP;
                float tv = lastNode.getSecond();
                long next = data.get(0).getFirst() + offset / TIME_STEP;
                float vStep = (data.get(0).getSecond() - tv) / (next - ts);
                ts += 1;
                tv += vStep;
                while (next > ts) {
                    values.add(tv);
                    ts++;
                    tv += vStep;
                }
            }
            for (Pair<Long, Float> node : data) {
                values.add(node.getSecond());
            }
        }
        return values;
    }

    public Pair<Long, Float> getLastNode() {
        if (data.size() <= 0)
            return null;
        return data.get(data.size() - 1);
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public void setData(List<Pair<Long, Float>> data) {
        List<Pair<Long, Float>> tmp = new ArrayList<>();
        tmp.add(data.get(0));
        for (int i = 1; i < data.size(); i++) {
            long timestamp = data.get(i - 1).getFirst();
            float value = data.get(i - 1).getSecond();
            long tmpNum = (data.get(i).getFirst() - timestamp);
            if (tmpNum > 1) {
                float tmpValue = (data.get(i).getSecond() - value) / tmpNum;
                while (tmpNum-- > 1) {
                    timestamp++;
                    value += tmpValue;
                    tmp.add(new Pair<>(timestamp, value));
                }
            }
            tmp.add(data.get(i));
        }
        this.data = tmp;
    }
}
