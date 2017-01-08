package cn.edu.fudan.dsm.basic.operator;

import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesRowKey;
import cn.edu.fudan.dsm.basic.common.util.TableHandler;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by huibo on 2016/12/7.
 */
public class TimeSeriesOperator implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesOperator.class.getName());

    private String channelCode;
    private int Wr;

    private TableHandler tableHandler;

    private Map<Long, TimeSeriesNode> cache = new HashMap<>();

    public TimeSeriesOperator(String channelCode, int Wr, boolean rebuild) throws IOException {
        this.channelCode = channelCode;
        this.Wr = Wr;

        TableName tableName = TableName.valueOf("histtable");
        HTableDescriptor tableDescriptors = new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("value");
        tableDescriptors.addFamily(columnDescriptor);

        // avoid auto split
//        tableDescriptors.setRegionSplitPolicyClassName(ConstantSizeRegionSplitPolicy.class.getName());
//
//        byte[][] splitKeys = new byte[1][];
//        for (int i = 0; i < splitKeys.length; i++) {
//            splitKeys[i] = Bytes.toBytes(new TimeSeriesRowKey(this.channelCode, (i + 1)).toString());
//        }

        tableHandler = new TableHandler(tableName, tableDescriptors, null, rebuild, false);
    }

    /**
     * get the first or last rowKey of the time series node
     *
     * @param firstOrLast false means get the first rowKey,
     *                    true means get the last rowKey.
     * @return The String type of the rowKey.
     * @throws IOException IOException
     */
    public String getFirstRowKey(boolean firstOrLast) throws IOException {
        Scan scan = new Scan().setCaching(1);
        if (firstOrLast)
            scan = scan.setReversed(true).setStopRow(Bytes.toBytes(channelCode))
                    .setStartRow(Bytes.toBytes(channelCode + "_" + Long.MAX_VALUE));
        else
            scan = scan.setReversed(false).setStartRow(Bytes.toBytes(channelCode))
                    .setStopRow(Bytes.toBytes(channelCode + "_" + Long.MAX_VALUE));
        ResultScanner scanner = getTable().getScanner(scan);
        Result result = scanner.next();
        return Bytes.toString(result.getRow());
    }

    public void close() throws IOException {
        tableHandler.close();
    }

    /**
     * Call the BufferedMutator's flush() to ensure all data has been written into HBase before further test
     * Write buffer should be enabled
     */
//    public void flushWriteBuffer() {
//        try {
//            tableHandler.flushWriteBuffer();
//        } catch (IOException e) {
//            logger.error(e.getMessage(), e.getCause());
//        }
//    }

    /**
     * Get a time series node from HBase data table
     *
     * @param rowKey row key of the time series node
     * @return the time series node instance, or null if not exists
     */
    public TimeSeriesNode getTimeSeriesNode(TimeSeriesRowKey rowKey) {
        logger.debug("Getting from HBase table: {}", rowKey);

        Get get = new Get(Bytes.toBytes(rowKey.toString()));
        get.addColumn(Bytes.toBytes("value"), Bytes.toBytes("data"));

        Result result = null;
        try {
            result = tableHandler.get(get);
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }

        TimeSeriesNode node = null;
        if (result != null) {
            node = new TimeSeriesNode();
            node.parseBytes(rowKey.getFirst(), result.getValue(Bytes.toBytes("value"), Bytes.toBytes("data")));
        }
        return node;
    }

    public Table getTable() {
        return tableHandler.getTable();
    }

    public RegionLocator getRegionLocator() {
        return tableHandler.getRegionLocator();
    }

    // rowKey = channelCode + offset * TimeSeriesNode.TIME_STEP
    public List<Float> getTimeSeries(long left, long length) {
        List<Float> ret = new ArrayList<>();

        long begin = left / Wr * Wr;
        long end = (left + length - 1) / Wr * Wr;

        if (begin == end) {  // `get` is faster
            TimeSeriesNode node = cache.get(begin);  // try to use cache first
            if (node == null) {
                node = getTimeSeriesNode(new TimeSeriesRowKey(channelCode, begin * TimeSeriesNode.TIME_STEP));
                cache.clear();
                cache.put(begin, node);
            }
            int offset = (int) (left % Wr);
            List<Pair<Long, Float>> nodeData = node.getData();
            int so = nodeData.get(0).getFirst().intValue();
            int eo = nodeData.size();
            for (int i = offset < so ? so : offset; i < Math.min(offset + length, eo); i++) {
                ret.add(nodeData.get(i).getSecond());
            }
        } else {  // use `scan` instead
            Scan scan = new Scan(Bytes.toBytes(new TimeSeriesRowKey(channelCode, begin * TimeSeriesNode.TIME_STEP).toString()),
                    Bytes.toBytes(new TimeSeriesRowKey(channelCode, (end + 1) * TimeSeriesNode.TIME_STEP).toString()));
            scan.addColumn(Bytes.toBytes("value"), Bytes.toBytes("data"));
            try (ResultScanner results = tableHandler.getTable().getScanner(scan)) {
                Pair<Long, Float> lastNode = null;
                long lastOffset = 0;
                for (Result result : results) {
                    TimeSeriesNode node = new TimeSeriesNode();
                    TimeSeriesRowKey tmpRowKey = new TimeSeriesRowKey(result.getRow());
                    node.parseBytes(tmpRowKey.getFirst(), result.getValue(Bytes.toBytes("value"), Bytes.toBytes("data")));

//                        String tmpRowKey = Bytes.toString(result.getRow());
                    long first = tmpRowKey.getFirst() / TimeSeriesNode.TIME_STEP; // the offset of the first node.(timestamp / timeStep)
                    if (first == begin) {
                        int offset = (int) (left % Wr);
                        List<Pair<Long, Float>> nodeData = node.getData();
                        int so = nodeData.get(0).getFirst().intValue();
                        int eo = nodeData.size();
                        for (int i = offset < so ? so : offset; i < Math.min(offset + length, eo); i++) {
                            ret.add(nodeData.get(i).getSecond());
                        }
                    } else if (first == end) {
                        cache.clear();
                        cache.put(first, node);

                        int offset = (int) ((left + length - 1) % Wr) + Wr - lastNode.getFirst().intValue();
                        List<Float> nodeData = node.getData(lastNode, lastOffset);
                        int eo = nodeData.size();
                        for (int i = 0; i < Math.min(offset, eo); i++) {
                            ret.add(nodeData.get(i));
                        }
                    } else {
                        ret.addAll(node.getData(lastNode, lastOffset));
                    }
                    lastNode = node.getLastNode();
                    lastOffset = tmpRowKey.getFirst();
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e.getCause());
            }
        }
        return ret;
    }

}
