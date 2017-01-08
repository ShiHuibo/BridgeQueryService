package cn.edu.fudan.dsm.basic.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Created by huibo on 2016/12/7.
 */
public class TableHandler implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TableHandler.class.getName());
    private Configuration conf = HBaseConfiguration.create();

    private Connection connection = null;

    private Admin admin = null;
    private Table table = null;
    private TableName tableName = null;

//    private boolean writeBuffer = true;

    private void createConnection() {
        String filePath = this.getClass().getResource("/")
                + "hbase-site.xml";
        conf.addResource(filePath);
        filePath = this.getClass().getResource("/")
                + "hdfs-site.xml";
        conf.addResource(filePath);
//        conf.addResource("localhbase/hbase-site.xml");
//        conf.addResource("localhbase/hdfs-site.xml");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            logger.trace("hbase connection is failed!");
            e.printStackTrace();
        }
    }

//    public boolean isWriteBuffer() {
//        return this.writeBuffer;
//    }

//    public void setWriteBuffer(boolean writeBuffer) {
//        this.writeBuffer = writeBuffer;
//    }

    public Table getTable() {
        return this.table;
    }

    public RegionLocator getRegionLocator() {
        try {
            return connection.getRegionLocator(tableName);
        } catch (IOException e) {
            logger.debug(e.getMessage(), e.getCause());
        }
        return null;
    }

    /**
     * split regions by splitKeys
     *
     * @param initialSplitKeys splitPoints which is the split point of the regions.
     * @throws IOException          IOException
     * @throws InterruptedException Interrupted
     */
    private void splitRegion(byte[][] initialSplitKeys) throws IOException, InterruptedException {
        byte[][] startKeys = getRegionLocator().getStartKeys();
        byte[] firstKey = startKeys[1], lastKey = startKeys[startKeys.length - 1];
        HRegionInfo region;
        boolean isBegin = true;
        for (byte[] key : initialSplitKeys) {
            if (Long.parseLong(Bytes.toString(key)) < Long.parseLong(Bytes.toString(firstKey)) && isBegin) {
                region = getRegionLocator().getRegionLocation(key, true).getRegionInfo();
                admin.splitRegion(region.getRegionName(), key);
            }
            if (Long.parseLong(Bytes.toString(key)) > Long.parseLong(Bytes.toString(lastKey)) && !isBegin) {
                region = getRegionLocator().getRegionLocation(key, true).getRegionInfo();
                admin.splitRegion(region.getRegionName(), key);
            }
            if (Long.parseLong(Bytes.toString(key)) == Long.parseLong(Bytes.toString(firstKey)))
                isBegin = false;
        }
    }

    /**
     * Initialize the connection and make sure the table is available
     *
     * @param tableName        the table to handle
     * @param tableDescriptors the description of the table
     * @param initialSplitKeys initial split the table based on the row keys to balance
     * @param rebuild          rebuild or not
     * @throws IOException
     */
    public TableHandler(TableName tableName, HTableDescriptor tableDescriptors, byte[][] initialSplitKeys, boolean rebuild, boolean reSplit)
            throws IOException {
        this.tableName = tableName;

        createConnection();
        admin = connection.getAdmin();

        if (rebuild) {
            createTableIfNotExists(tableDescriptors, initialSplitKeys);
        }
        if (reSplit) {
            try {
                splitRegion(initialSplitKeys);
            } catch (InterruptedException e) {
                logger.debug(e.getMessage(), e.getCause());
            }
        }

        table = connection.getTable(tableName);
    }

    private void createTableIfNotExists(HTableDescriptor tableDescriptors, byte[][] initialSplitKeys) throws IOException {
        logger.trace("Begin checking HBase table `{}`", tableName);


        if (admin.tableExists(tableName)) {
            if (!admin.isTableDisabled(tableName)) {
                logger.info("Disabling HBase table `{}`", tableName);
                admin.disableTable(tableName);
            }
            logger.info("Deleting HBase table `{}`", tableName);
            admin.deleteTable(tableName);
        }

        if (!admin.tableExists(tableName)) {
            logger.info("Creating HBase table `{}`", tableName);
            if (initialSplitKeys != null) {
                admin.createTable(tableDescriptors, initialSplitKeys);
            } else {
                admin.createTable(tableDescriptors);
            }
        }

        if (!admin.isTableEnabled(tableName)) {
            logger.info("Enabling HBase table `{}`", tableName);
            admin.enableTable(tableName);
        }
        logger.trace("Finish checking HBase table `{}`", tableName);
    }

    /**
     * Call the BufferedMutator's flush() to ensure all data has been written into HBase before further test
     * Write buffer should be enabled
     */
//    public void flushWriteBuffer() throws IOException {
//        if (writeBuffer) {
//            table.flushCommits();
//        }
//    }
    public void put(Put put) throws IOException {
        table.put(put);
    }

    public void put(List<Put> puts) throws IOException {
        table.put(puts);
    }

    public Result get(Get get) throws IOException {
        return table.get(get);
    }

    @Override
    public void close() throws IOException {
        if (table != null)
            table.close();
        if (admin != null)
            admin.close();
        if (connection != null)
            connection.close();
    }
}
