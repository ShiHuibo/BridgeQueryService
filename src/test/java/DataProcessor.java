import cn.edu.fudan.dsm.basic.common.entity.IndexNode;
import cn.edu.fudan.dsm.basic.common.entity.IndexRowKey;
import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesRowKey;
import cn.edu.fudan.dsm.basic.executor.BuildIndexExecutor;
import cn.edu.fudan.dsm.basic.operator.IndexOperator;
import cn.edu.fudan.dsm.basic.operator.TimeSeriesOperator;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by huibo on 2016/12/19.
 */
public class DataProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DataProcessor.class);
    private HBaseUtil hBaseUtil = new HBaseUtil();

    private TableName histTableName = TableName.valueOf("histtable");
    private byte[] histFamilyName = Bytes.toBytes("value");
    private byte[] histQualifierName = Bytes.toBytes("data");
    private TableName indexTableName = TableName.valueOf("indextable");
    private byte[] indexFamilyName = Bytes.toBytes("std");
    private byte[] indexQualifierName = Bytes.toBytes("p");

    /**
     * move the data from hbase to hbase
     * <p>
     * //     * @param args  tableName channelCode
     *
     * @throws Exception
     */
    @Test
    public void migrateData() throws Exception {
        hBaseUtil.getAdmin();

        String start = "8ST024-D" + "_" + 0l;
        String end = "8ST024-D" + "_" + System.currentTimeMillis();

        Table tableIn = hBaseUtil.getTable(hBaseUtil.adminIn, histTableName, histFamilyName);
        Table tableOut = hBaseUtil.getTable(hBaseUtil.adminOut, histTableName, histFamilyName);

        ResultScanner resultScanner = hBaseUtil.getData(Bytes.toBytes(start), Bytes.toBytes(end), tableIn);
        hBaseUtil.moveData(resultScanner, tableOut, histFamilyName, histQualifierName);
    }

    /**
     * move the index from hbase to hbase.
     * note: region to region
     *
     * @throws Exception
     */
    @Test
    public void migrateDataByRegion() throws Exception {
        hBaseUtil.getAdmin();

        Table tableIn = hBaseUtil.getTable(hBaseUtil.adminOut, indexTableName, indexFamilyName);
        byte[][] tableInStartKeys = hBaseUtil.adminOut.getConnection().getRegionLocator(indexTableName).getStartKeys();
        byte[][] tableOutSplitKeys = new byte[tableInStartKeys.length - 1][];
        for (int i = 1; i < tableInStartKeys.length; i++) {
            tableOutSplitKeys[i - 1] = tableInStartKeys[i];
        }
        Table tableOut = hBaseUtil.getTable(hBaseUtil.adminIn, indexTableName, indexFamilyName, tableOutSplitKeys);
        ResultScanner resultScanner = hBaseUtil.getData(Bytes.toBytes("0"), Bytes.toBytes("9"), tableIn);
        hBaseUtil.moveData(resultScanner, tableOut, indexFamilyName, indexQualifierName);
    }

}
