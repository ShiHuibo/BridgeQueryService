import cn.edu.fudan.dsm.basic.common.entity.IndexNode;
import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huibo on 2017/1/8.
 */
public class HBaseUtil {

    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);
    private Configuration confIn = HBaseConfiguration.create();
    private Configuration confOut = HBaseConfiguration.create();

    Admin adminIn = null;
    Admin adminOut = null;

    HBaseUtil() {
        confOut.addResource(new Path(DataProcessor.class.getResource("localhbase/hdfs-site.xml").getPath()));
        confOut.addResource(new Path(DataProcessor.class.getResource("localhbase/hbase-site.xml").getPath()));
    }

    public void getAdmin() throws IOException {
        Connection connIn = ConnectionFactory.createConnection(confIn);
        Connection connOut = ConnectionFactory.createConnection(confOut);
        adminIn = connIn.getAdmin();
        adminOut = connOut.getAdmin();
    }

    public Table getTable(Admin admin, TableName tableName, byte[] familyName)
            throws IOException {
        if (!admin.tableExists(tableName)) {
            HTableDescriptor htd = new HTableDescriptor(tableName);
            HColumnDescriptor hcd = new HColumnDescriptor(familyName);
            htd.addFamily(hcd);
            admin.createTable(htd);
        }
        return admin.getConnection().getTable(tableName);
    }

    public Table getTable(Admin admin, TableName tableName, byte[] familyName, byte[][] splitKeys)
            throws IOException, InterruptedException {
        if (!admin.tableExists(tableName)) {
            HTableDescriptor htd = new HTableDescriptor(tableName);
            HColumnDescriptor hcd = new HColumnDescriptor(familyName);
            htd.addFamily(hcd);
            admin.createTable(htd, splitKeys);
        } else {
            splitRegion(admin, tableName, splitKeys);
        }
        return admin.getConnection().getTable(tableName);
    }

    /**
     * split regions by splitKeys
     *
     * @param initialSplitKeys splitPoints which is the split point of the regions.
     * @throws IOException          IOException
     * @throws InterruptedException Interrupted
     */
    private void splitRegion(Admin admin, TableName tableName, byte[][] initialSplitKeys)
            throws IOException, InterruptedException {
        byte[][] startKeys = admin.getConnection().getRegionLocator(tableName).getStartKeys();
        byte[] firstKey = startKeys[1], lastKey = startKeys[startKeys.length - 1];
        HRegionInfo region;
        boolean isBegin = true;
        for (byte[] key : initialSplitKeys) {
            if (Long.parseLong(Bytes.toString(key)) < Long.parseLong(Bytes.toString(firstKey)) && isBegin) {
                region = admin.getConnection().getRegionLocator(tableName).getRegionLocation(key, true).getRegionInfo();
                admin.splitRegion(region.getRegionName(), key);
            }
            if (Long.parseLong(Bytes.toString(key)) > Long.parseLong(Bytes.toString(lastKey)) && !isBegin) {
                region = admin.getConnection().getRegionLocator(tableName).getRegionLocation(key, true).getRegionInfo();
                admin.splitRegion(region.getRegionName(), key);
            }
            if (Long.parseLong(Bytes.toString(key)) == Long.parseLong(Bytes.toString(firstKey)))
                isBegin = false;
        }
    }

    public void moveData(ResultScanner scannerIn, Table tableOut, byte[] familyName, byte[] qualifierName)
            throws IOException {
        long i = 1;
        List<Put> puts = new ArrayList<>();
        for (Result result : scannerIn) {
            byte[] rowKey = result.getRow();
            byte[] value = result.getValue(familyName, qualifierName);
            if (value == null) continue;
            Put put = new Put(rowKey);
            put.addColumn(familyName, qualifierName, value);
            puts.add(put);
            if (i % 500 == 0) {
                logger.info("Current Number : " + i, put);
                tableOut.put(puts);
                puts.clear();
            }
            i++;
        }
        if (puts.size() > 0) {
            tableOut.put(puts);
        }
        logger.info("{} row(s).", i - 1);
        puts.clear();
    }

    public ResultScanner getData(byte[] startRowKey, byte[] endRowKey, Table tableIn)
            throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(startRowKey);
        scan.setStopRow(endRowKey);
        scan.setCaching(500);
        return tableIn.getScanner(scan);
    }

    public ResultScanner getDataByRowFilter(Table tableIn, String rowKey)
            throws IOException {
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(rowKey));
        Scan scan = new Scan();
        scan.setFilter(filter);
        scan.setCaching(500);
        return tableIn.getScanner(scan);
    }

    public void printIndexNode(Result result, byte[] familyName, byte[] qualifierName) {
        String rowKey = Bytes.toString(result.getRow());
        IndexNode node = new IndexNode();
        node.parseBytes(result.getValue(familyName, qualifierName));
        logger.info("Data:{ RowKey => '{}', Value = '{}'.", rowKey, node.toString());
    }

    public void printTimeSeriesNode(Result result, byte[] familyName, byte[] qualifierName) throws IOException {
        String rowKey = Bytes.toString(result.getRow());
        TimeSeriesNode node = new TimeSeriesNode();
        node.parseBytes(Long.parseLong(rowKey.split("_")[1]), result.getValue(familyName, qualifierName));
        if (node.getData().size() != 60)
            logger.info("Node:{ RowKey => '{}', Length => '{}', Value => '{}'.",
                    rowKey, node.getData().size(), node.toString());
    }
}
