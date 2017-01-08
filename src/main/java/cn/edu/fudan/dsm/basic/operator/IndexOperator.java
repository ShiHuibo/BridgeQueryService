package cn.edu.fudan.dsm.basic.operator;

import cn.edu.fudan.dsm.basic.common.entity.IndexNode;
import cn.edu.fudan.dsm.basic.common.util.TableHandler;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Calendar;
import java.util.List;

/**
 * Created by huibo on 2016/12/7.
 */
public class IndexOperator implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(IndexOperator.class.getName());

    private TableHandler tableHandler;

    public IndexOperator(long startTime, long endTime, boolean rebuild, boolean reSplit) throws IOException {
        TableName tableName = TableName.valueOf("indextable");
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("std");
        tableDescriptor.addFamily(columnDescriptor);

        // avoid auto split
        byte[][] splitKeys = new byte[1][];
        if (rebuild || reSplit) {
            tableDescriptor.setRegionSplitPolicyClassName(ConstantSizeRegionSplitPolicy.class.getName());

            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(endTime);
            int endYear = calendar.get(Calendar.YEAR);
            int endMonth = calendar.get(Calendar.MONTH);
            calendar.setTimeInMillis(startTime);
            int startYear = calendar.get(Calendar.YEAR);
            int startMonth = calendar.get(Calendar.MONTH);
            calendar.set(Calendar.DAY_OF_MONTH,1);
            calendar.set(Calendar.HOUR_OF_DAY,0);
            calendar.set(Calendar.MINUTE,0);
            calendar.set(Calendar.SECOND,0);
            calendar.set(Calendar.MILLISECOND,0);
            splitKeys = new byte[(endYear - startYear) * 12 + endMonth - startMonth][];
            for (int i = 0; i < splitKeys.length; i++) {
                splitKeys[i] = Bytes.toBytes(String.valueOf(calendar.getTimeInMillis()));
                calendar.add(Calendar.MONTH, 1);
            }
        }

        tableHandler = new TableHandler(tableName, tableDescriptor, splitKeys, rebuild, reSplit);
    }

    @Override
    public void close() throws IOException {
        tableHandler.close();
    }

    public IndexNode getIndexNode(String rowKey) {
        logger.debug("Getting from hbase table : {}", rowKey);

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes("std"));

        Result result = null;
        try {
            result = tableHandler.get(get);
        } catch (IOException e) {
            logger.debug(e.getMessage(), e.getCause());
        }

        IndexNode node = null;
        if (result != null) {
            node = new IndexNode();
            node.parseBytes(result.getValue(Bytes.toBytes("std"), Bytes.toBytes("p")));
        }
        return node;
    }

    public void put(Put put) {
        try {
            tableHandler.put(put);
        } catch (IOException e) {
            logger.debug(e.getMessage(), e.getCause());
        }
    }

    public void putList(List<Put> puts) {
        try {
            tableHandler.put(puts);
        } catch (IOException e) {
            logger.debug(e.getMessage(), e.getCause());
        }
    }

    public RegionLocator getRegionLocator() {
        return tableHandler.getRegionLocator();
    }

    public Table getTable() {
        return tableHandler.getTable();
    }
}
