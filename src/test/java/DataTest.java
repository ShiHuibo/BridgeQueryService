import cn.edu.fudan.dsm.basic.common.entity.IndexNode;
import cn.edu.fudan.dsm.basic.common.entity.IndexRowKey;
import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesRowKey;
import cn.edu.fudan.dsm.basic.operator.TimeSeriesOperator;
import com.sun.org.apache.xerces.internal.impl.dv.util.*;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by huibo on 2017/1/8.
 */
public class DataTest {

    private static final Logger logger = LoggerFactory.getLogger(DataTest.class);

    private HBaseUtil hBaseUtil = new HBaseUtil();
    private TableName histTableName = TableName.valueOf("histtable");
    private byte[] histFamilyName = Bytes.toBytes("value");
    private byte[] histQualifierName = Bytes.toBytes("data");
    private TableName indexTableName = TableName.valueOf("indextable");
    private byte[] indexFamilyName = Bytes.toBytes("std");
    private byte[] indexQualifierName = Bytes.toBytes("p");

    @Test
    public void printDatas() throws IOException {
        hBaseUtil.getAdmin();

        Table histTable = hBaseUtil.getTable(hBaseUtil.adminIn, histTableName, histFamilyName);
//        tableName = TableName.valueOf("indextable");
//        familyName = Bytes.toBytes("std");
//        Table indexTable = hBaseUtil.getTable(hBaseUtil.adminOut, tableName, familyName);
        String[][] items = {
//                {"SCTW410-D", "1/60 Hz :"},     // no data
//                {"8AV010-DY", "1/50 Hz :"},
//                {"SAV0201-DZ", "1/50 Hz :"},    // no data
//                {"8SZ006-D", "1/40 Hz :"},
//                {"8EV010-D", "1/25 Hz :"},
//                {"SCH0603-SY", "1/20 Hz :"},    // no data
//                {"8GP004-DZ", "1/10 Hz :"},
//                {"SGP1101-DZ", "1/5 Hz :"},     // no data
//                {"SGP1101-DX", "1/2 Hz :"},     // no data
                {"8ST024-D", "1 Hz :"},
//                {"8WS001-DT", "60 Hz :"},
//                {"8ST015-S", "600 Hz :"}
        };
        for (String[] item : items) {
            String startRowKey = item[0];
            String endRowKey = item[0] + "_9";
            Scan scan = new Scan(Bytes.toBytes(startRowKey), Bytes.toBytes(endRowKey)).setCaching(500);
//            ResultScanner resultScanner = histTable.getScanner(scan);
//            for (int i = 0; i < 10; i++) {
//                Result result = resultScanner.next();
//                if (result == null) break;
//                TimeSeriesRowKey timeSeriesRowKey = new TimeSeriesRowKey(Bytes.toString(result.getRow()));
//                logger.info("{} {} : {}", item[1], new Date(timeSeriesRowKey.getFirst()), result.getValue(histFamilyName, histQualifierName).length / 16);
//            }
            try (ResultScanner resultScanner = histTable.getScanner(scan)) {
                Result result;
                int i = -1;
                do {
                    result = resultScanner.next();
                    if (result != null) {
                        logger.info("row {}, value {}", Bytes.toString(result.getRow()), Bytes.toString(result.getValue(histFamilyName, histQualifierName)));
                    }
                    i++;
                } while (result != null);
                logger.info("all num is {}", i);
            }
        }

//        familyName = Bytes.toBytes("std");
//        qualifierName = Bytes.toBytes("p");

//        ResultScanner resultScanner = getDataByRowFilter(indexTable, rowKey);
//        ResultScanner resultScanner = getData(Bytes.toBytes(startRowKey), Bytes.toBytes(endRowKey), indexTable);
//
//        String rowKey, tmpRowKey = null;
//        long a, tmpa = 0;
//        TimeSeriesNode node, tmpNode = null;
//        for (Result result : resultScanner) {
//            rowKey = Bytes.toString(result.getRow());
//            node = new TimeSeriesNode();
//            a = Long.parseLong(rowKey.split("_")[1]);
//            node.parseBytes(a, result.getValue(familyName, qualifierName));
//            if (tmpRowKey != null) {
//                tmpa = a - Long.parseLong(tmpRowKey.split("_")[1]);
//            }
//            if (tmpa > 60000)
//                logger.info("preRegion is {}, nextRegion is {}, tmpa = {}.", tmpRowKey, rowKey, tmpa);
//            if (node.getData().size() != 60) {
//                if (tmpRowKey == null) {
//                    tmpNode = node;
//                    tmpRowKey = rowKey;
//                }
//                logger.info("\ntmpNode:{ RowKey => '{}', Length => '{}', Value => '{}'.\nNode:{ RowKey => '{}', Length => '{}', Value => '{}'.",
//                        tmpRowKey, tmpNode.getData().size(), tmpNode.toString(), rowKey, node.getData().size(), node.toString());
//
//                TimeSeriesOperator timeSeriesOperator = new TimeSeriesOperator("8ST024-D", 60, false);
//                List<Float> re = timeSeriesOperator.getTimeSeries(new TimeSeriesRowKey(tmpRowKey).getFirst() / TimeSeriesNode.TIME_STEP, 1000);
//                logger.info("data {}\n", re.size());
//            }
//
//            tmpNode = node;
//            tmpRowKey = rowKey;
////            allRowKey = allRowKey + "," + tmpa;
//        }
//        logger.info("rowkeys {}", allRowKey);
    }

    @Test
    public void printIndex() throws IOException {
        hBaseUtil.getAdmin();

        Table indexTable = hBaseUtil.getTable(hBaseUtil.adminOut, indexTableName, indexFamilyName);
        Table histTable = hBaseUtil.getTable(hBaseUtil.adminIn, histTableName, histFamilyName);

        byte[] startRow = Bytes.toBytes("8ST024-D" + "_" + 1167580800000l);
        byte[] stopRow = Bytes.toBytes("8ST024-D" + "_" + 1172678400000l);
        Scan scan = new Scan().setStartRow(startRow).setStopRow(stopRow).setCaching(500);
        ResultScanner resultHistScanner = histTable.getScanner(scan);
        long histnum = 0;
        for (Result r : resultHistScanner) {
            histnum += r.getValue(histFamilyName, histQualifierName).length / 16;
        }
        logger.info("\n\n histnum is {}.\n", histnum);

        startRow = Bytes.toBytes(1167580800000l + "_");
        stopRow = Bytes.toBytes(9967580800001l + "_");
        scan = new Scan().setStartRow(startRow).setStopRow(stopRow).setCaching(500);
        ResultScanner resultScanner = indexTable.getScanner(scan);
        long minnum = Long.MAX_VALUE, num = 0, n = 0, maxnum = Long.MIN_VALUE;
        String row = "";
        for (Result r : resultScanner) {
            IndexRowKey rowKey = new IndexRowKey(Bytes.toString(r.getRow()));
            String tmprow = rowKey.getFirst() + "_" + rowKey.getChannelCode();
            IndexNode node = new IndexNode();
            node.parseBytes(r.getValue(indexFamilyName, indexQualifierName));
            List<Pair<Integer, Integer>> list = node.getPositions();
            int size = list.size();
            if (!row.equals(tmprow)) {
                if (!row.isEmpty())
                    logger.info("\n\n minnum is {}, num is {}, maxnum is {}.\n", minnum, num, maxnum);
                row = tmprow;
                minnum = Long.MAX_VALUE;
                maxnum = Long.MIN_VALUE;
            }
            if (!rowKey.getValue().equals("statistic")) {
                for (Pair<Integer, Integer> pair : list) {
                    num += (pair.getSecond() - pair.getFirst() + 1);
                    n++;
                }
                minnum = minnum < list.get(0).getFirst() ? minnum : list.get(0).getFirst();
                maxnum = maxnum > list.get(size - 1).getSecond() ? maxnum : list.get(size - 1).getSecond();
            } else if (rowKey.getValue().equals("statistic")) {
                for (Pair<Integer, Integer> pair : list) {
                    logger.info("list {}", pair.toString());
                }
            }
            logger.info("\n RowKey : {} , number : {}, values : {}.", row, size, node.toString());
        }
        logger.info("\n\n minnum is {}, num is {},n is {}, maxnum is {}.\n", minnum, num, n, maxnum);
    }

    @Test
    public void coprocessorTest() throws IOException {
        hBaseUtil.getAdmin();

        Table histTable = hBaseUtil.getTable(hBaseUtil.adminIn, histTableName, histFamilyName);
        Table indexTable = hBaseUtil.getTable(hBaseUtil.adminOut, indexTableName, indexFamilyName);
        byte[] startRow = Bytes.toBytes("8ST024-D" + "_");
//        byte[] startRow = Bytes.toBytes("8ST024-D" + "_" + 1170259200000l);
//        byte[] stopRow = Bytes.toBytes("8ST024-D" + "_" + 1170259200000l);
        byte[] stopRow = Bytes.toBytes("8ST024-D" + "_" + 1199116800000l);
        Scan scan = new Scan().setStartRow(startRow).setStopRow(stopRow).setCaching(500);
        ResultScanner resultScanner = histTable.getScanner(scan);
//        IndexBuilder indexBuilder = new IndexBuilder("8ST024-D", 60, 50, 5,
//                1167580800000l, 1170259200000l, resultScanner, indexTable);
        IndexBuilder indexBuilder = new IndexBuilder("8ST024-D", 60, 50, 5,
                1167580800000l, 1199116800000l, resultScanner, indexTable);
//        IndexBuilder indexBuilder = new IndexBuilder("8ST024-D", 60, 50, 5,
//                1170259200000l, 1172678400000l, resultScanner, indexTable);
//        addRegionsIndex("8ST024-D", 60, 50, 5, indexTable);
        indexBuilder.run();
    }

    private void addRegionsIndex(String channelCode, int Wr, int Wu, int Ur, Table table) throws IOException {
        byte[][] startKeys = {
                Bytes.toBytes(channelCode + "_" + 1167580800000l),
                Bytes.toBytes(channelCode + "_" + 1170259200000l),
                Bytes.toBytes(channelCode + "_" + 1172678400000l)};
        for (int i = 1; i < startKeys.length; i++) {
            TimeSeriesRowKey tmpRowKey = new TimeSeriesRowKey(Bytes.toString(startKeys[i]));
            if (!tmpRowKey.getChannelCode().equals(channelCode)) continue;
            long regionSplit = tmpRowKey.getFirst() / TimeSeriesNode.TIME_STEP; // the offset of the first node.(timestamp / timeStep)
            TimeSeriesOperator timeSeriesOperator = new TimeSeriesOperator("8ST024-D", 60, false);
            List<Float> data = timeSeriesOperator.getTimeSeries(regionSplit - Wu + 1, 2 * Wu - 2);  // [s-Wu+1, s+Wu-2]
            if (data.size() >= Wu) {
                IndexBuilder1 indexBuilder = new IndexBuilder1(channelCode, Wu, Ur, regionSplit, regionSplit - Wu + 1, data, table);
                indexBuilder.run();
            }

//            long nextRowBegin = (regionSplit + Wr - 2) / Wr * Wr + 1;
//            data = timeSeriesOperator.getTimeSeries(regionSplit, (int) (nextRowBegin - regionSplit + Wu - 1 + 1));  // TODO  // [s, t+Wu-2]
//            if (data.size() >= Wu) {
//                IndexBuilder1 indexBuilder = new IndexBuilder1(channelCode, Wu, Ur, -1, regionSplit, data, table);
//                indexBuilder.run();
//            }
        }
    }

    private static class IndexBuilder1 {

        String channelCode;
        long first = -1;      // left offset, for output global position of time series
        Table table;
        List<Float> data;
        int Ur;
        long regionSplit;

        double[] t;  // data array and query array

        double d;
        double ex, ex2, mean, std;
        int Wu = -1;
        double[] buffer;

        // For every EPOCH points, all cumulative values, such as ex (sum), ex2 (sum square), will be restarted for reducing the floating point error.
        int EPOCH = 100000;

        int dataIndex = -1;
        int missNum = 0;
        long splitPoint, regionBegin;

        IndexBuilder1(String channelCode, int Wu, int Ur, long regionSplit, long first, List<Float> data, Table table) throws IOException {
            this.channelCode = channelCode;
            this.Wu = Wu;
            this.Ur = Ur;
            this.regionSplit = regionSplit;
            this.data = data;
            this.first = first;
            this.splitPoint = first;

            t = new double[this.Wu * 2];
            buffer = new double[EPOCH];

            this.table = table;
        }

        boolean nextData() throws IOException {
            if (dataIndex + 1 < data.size()) {
                dataIndex++;
                return true;
            } else {
                return false;
            }
        }

        Float getCurrentData() {
            return data.get(dataIndex);
        }

        long getStatisticSplitPoint(int amount) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(splitPoint * TimeSeriesNode.TIME_STEP);
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            calendar.add(Calendar.MONTH, amount);
            return calendar.getTimeInMillis() / TimeSeriesNode.TIME_STEP;
        }

        void run() throws IOException {
            boolean done = false;
            int it = 0, ep;
            regionBegin = getStatisticSplitPoint(0);
            splitPoint = getStatisticSplitPoint(1);

            String lastMeanRound = null;
            IndexNode indexNode = null;
            Map<String, IndexNode> indexNodeMap = new HashMap<>();

            while (!done) {
                // Read first Wu-1 points
                int k;
                if (it == 0) {
                    for (k = 0; k < Wu - 1; k++) {
                        if (nextData()) {
                            d = getCurrentData();
                            buffer[k] = d;
                        } else
                            break;
                    }
                } else {
                    for (k = 0; k < Wu - 1; k++) {
                        buffer[k] = buffer[EPOCH - Wu + 1 + k];
                    }
                }

                // Read buffer of size EPOCH or when all data has been read.
                ep = k;
                while (ep < EPOCH) {
                    if (nextData()) {
                        d = getCurrentData();
                        buffer[ep] = d;
                        ep++;
                    } else {
                        break;
                    }
                }

                // Data are read in chunk of size EPOCH.
                // When there is nothing to read, the loop is end.
                if (ep <= Wu - 1) {
                    missNum += ep;
                    // put the last node
                    if (indexNode != null && !indexNode.getPositions().isEmpty()) {
                        indexNodeMap.put(lastMeanRound, indexNode);
                    }

                    lastMeanRound = null;
                    indexNode = null;
                    done = true;
                } else {
                    ex = 0;
                    ex2 = 0;
                    for (int i = 0; i < ep; i++) {
                        // A bunch of data has been read and pick one of them at a time to use
                        d = buffer[i];

                        // Calculate sum and sum square
                        ex += d;
                        ex2 += d * d;

                        // t is a circular array for keeping current data
                        t[i % Wu] = d;

                        // Double the size for avoiding using modulo "%" operator
                        t[(i % Wu) + Wu] = d;

                        // Start the task when there are more than m-1 points in the current chunk
                        if (i >= Wu - 1) {
                            mean = ex / Wu;
                            std = ex2 / Wu;
                            std = Math.sqrt(std - mean * mean);

                            // compute the start location of the data in the current circular array, t
                            int j = (i + 1) % Wu;

                            // store the mean and std for current chunk
                            long loc = (it) * (EPOCH - Wu + 1) + i - Wu + 1 + 1 + first - 1;
                            if (regionSplit != -1 && loc >= regionSplit) {
                                done = true;
                                break;
                            }
                            if (loc >= splitPoint) {
                                // put the last node
                                if (indexNode != null && !indexNode.getPositions().isEmpty()) {
                                    indexNodeMap.put(lastMeanRound, indexNode);
                                }

                                if (!indexNodeMap.isEmpty())
                                    putDataIntoHbase(indexNodeMap, regionBegin);

                                regionBegin = splitPoint;
                                splitPoint = getStatisticSplitPoint(1);

                                lastMeanRound = null;
                                indexNode = null;
                                indexNodeMap.clear();
                            }
                            String meanLong = String.valueOf(Double.doubleToLongBits(mean));
                            String curMeanRound = meanLong.charAt(0) == '-' ? meanLong.substring(0, Ur + 1) : meanLong.substring(0, Ur);
                            logger.debug("mean:{}({})[{}], std:{}, loc:{}", mean, meanLong, curMeanRound, std, loc);

                            if (lastMeanRound == null || !curMeanRound.equals(lastMeanRound)) {
                                // put the last row
                                if (lastMeanRound != null) {
                                    indexNodeMap.put(lastMeanRound, indexNode);
                                }
                                // new row
                                logger.debug("new row, rowkey: {}", curMeanRound);
                                indexNode = indexNodeMap.get(curMeanRound);
                                if (indexNode == null) {
                                    indexNode = new IndexNode();
                                }
                                indexNode.getPositions().add(new Pair<>((int) (loc - regionBegin), (int) (loc - regionBegin)));
                                lastMeanRound = curMeanRound;
                            } else {
                                // use last row
                                logger.debug("use last row, rowkey: {}", lastMeanRound);
                                int index = indexNode.getPositions().size();
                                indexNode.getPositions().get(index - 1).setSecond((int) (loc - regionBegin));
                            }

                            // Reduce obsolete points from sum and sum square
                            ex -= t[j];
                            ex2 -= t[j] * t[j];
                        }
                    }

                    // If the size of last chunk is less then EPOCH, then no more data and terminate.
                    if (ep < EPOCH) {
                        missNum += ep;
                        // put the last node
                        if (indexNode != null && !indexNode.getPositions().isEmpty()) {
                            indexNodeMap.put(lastMeanRound, indexNode);
                        }

                        lastMeanRound = null;
                        indexNode = null;
                        done = true;
                    } else {
                        it++;
                    }
                }
            }

            // put the last node
            if (indexNode != null && !indexNode.getPositions().isEmpty()) {
                indexNodeMap.put(lastMeanRound, indexNode);
            }

            putDataIntoHbase(indexNodeMap, regionBegin);
//            indexOperator.flushWriteBuffer();
        }

        void putDataIntoHbase(Map<String, IndexNode> indexNodeMap, long startTimestamp) throws IOException {
            List<Put> puts = new ArrayList<>(indexNodeMap.size());
            for (Map.Entry entry : indexNodeMap.entrySet()) {
                long startT = startTimestamp * TimeSeriesNode.TIME_STEP;
                String prefix = startT + "_" + channelCode + "_"; // the rowKey of index table likes timestampOfMonth_channelCode_mean
                Put put = new Put(Bytes.toBytes(prefix + entry.getKey()));
                IndexNode indexNodeCurrent = (IndexNode) entry.getValue();
                IndexNode indexNodeOrigin = new IndexNode();
                indexNodeOrigin.parseBytes(table.get(new Get(Bytes.toBytes(prefix + entry.getKey())))
                        .getValue(Bytes.toBytes("std"), Bytes.toBytes("p")));
                if (indexNodeOrigin != null) {
                    indexNodeOrigin.getPositions().addAll(indexNodeCurrent.getPositions());
                    indexNodeOrigin.setPositions(sortAndMergeIntervals(indexNodeOrigin.getPositions()));
                    put.addColumn(Bytes.toBytes("std"), Bytes.toBytes("p"), indexNodeOrigin.toBytes());
                } else {
                    put.addColumn(Bytes.toBytes("std"), Bytes.toBytes("p"), indexNodeCurrent.toBytes());
                }
                puts.add(put);
            }
            table.put(puts);
//            indexOperator.flushWriteBuffer();
        }

        private List<Pair<Integer, Integer>> sortAndMergeIntervals(List<Pair<Integer, Integer>> intervals) {
            if (intervals.size() <= 1) {
                return intervals;
            }

            Collections.sort(intervals, new Comparator<Pair<Integer, Integer>>() {
                @Override
                public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
                    return (int) (o1.getFirst() - o2.getFirst());
                }
            });

            Pair<Integer, Integer> first = intervals.get(0);
            int start = first.getFirst();
            int end = first.getSecond();

            List<Pair<Integer, Integer>> result = new ArrayList<>();

            for (int i = 1; i < intervals.size(); i++) {
                Pair<Integer, Integer> current = intervals.get(i);
                if (current.getFirst() - 1 <= end) {
                    end = Math.max(current.getSecond(), end);
                } else {
                    result.add(new Pair<>(start, end));
                    start = current.getFirst();
                    end = current.getSecond();
                }
            }
            result.add(new Pair<>(start, end));

            return result;
        }

    }

    private static class IndexBuilder {

        long first = -1;      // left offset, for output global position of time series
        ResultScanner scanner;
        int Ur;
        long regionBegin, regionEnd;
        boolean done = false;
        Table table;

        double[] t;  // data array and query array

        double d;
        double ex, ex2, mean, std;
        String channelCode;
        int Wr = -1, Wu = -1;
        double[] buffer;

        // For every EPOCH points, all cumulative values, such as ex (sum), ex2 (sum square), will be restarted for reducing the floating point error.
        int EPOCH = 100000;

        TimeSeriesNode node = new TimeSeriesNode();
        List<Float> nodeData = new ArrayList<>();
        Pair<Long, Float> lastNode = null;
        long lastOffset = 0;
        int dataIndex = 0;
        long missNum = 0;
        long tmpMissNum = 0;

        long allnum = 0;

        IndexBuilder(String channelCode, int Wr, int Wu, int Ur, long regionBegin, long regionEnd, ResultScanner scanner, Table table) throws IOException {
            this.scanner = scanner;
            this.Wr = Wr;
            this.Wu = Wu;
            this.channelCode = channelCode;
            this.Ur = Ur;
            this.regionBegin = regionBegin / TimeSeriesNode.TIME_STEP;
            this.regionBegin = getStatisticSplitPoint(0);
            this.regionEnd = regionEnd / TimeSeriesNode.TIME_STEP;
            this.table = table;

            t = new double[Wu * 2];
            buffer = new double[EPOCH];
        }

        boolean nextData() throws IOException {
            if (tmpMissNum > 3 * Wu)
                return false;
            if (dataIndex + 1 < nodeData.size()) {
                dataIndex++;
                return true;
            } else {
                if (!done) {
                    Result results = scanner.next();
                    done = results == null ? true : false;
                    if (done) return false;
                    long tmpOffset = new TimeSeriesRowKey(Bytes.toString(results.getRow())).getFirst();
                    node = new TimeSeriesNode();
                    try {
                        node.parseBytes(tmpOffset, results.getValue(Bytes.toBytes("value"), Bytes.toBytes("data")));
                    } catch (NullPointerException e) {
                        logger.info("rowkey is {}", Bytes.toString(results.getRow()));
                    }

                    if (first == -1) {
                        first = tmpOffset / TimeSeriesNode.TIME_STEP;
                    } else {
                        tmpMissNum = (tmpOffset - lastOffset) / TimeSeriesNode.TIME_STEP + node.getData().get(0).getFirst() - lastNode.getFirst() - 1;
                        if (tmpMissNum > 3 * Wu) {
                            nodeData = node.getData(lastNode, 0);
                            lastNode = node.getLastNode();
                            lastOffset = tmpOffset;
                            dataIndex = -1;
                            return false;
                        }
                    }
                    nodeData = node.getData(lastNode, lastOffset);
                    lastNode = node.getLastNode();
                    lastOffset = tmpOffset;
                    dataIndex = 0;
                    return true;
                } else {
                    return false;
                }
            }
        }

        double getCurrentData() {
            return nodeData.get(dataIndex);
        }

        void putDataIntoHbase(Map<String, IndexNode> indexNodeMap, long startTimestamp) throws IOException {
            // put the index data to HBase actually
            List<Put> puts = new ArrayList<>(indexNodeMap.size());
            List<Pair<String, Integer>> statisticInfo = new ArrayList<>(indexNodeMap.size());
            long startT = startTimestamp * TimeSeriesNode.TIME_STEP;
            String prefix = startT + "_" + channelCode + "_";
            for (Map.Entry entry : indexNodeMap.entrySet()) {
                Put put = new Put(Bytes.toBytes(prefix + entry.getKey()));
                IndexNode indexNode1 = (IndexNode) entry.getValue();
                for (Pair<Integer, Integer> item : indexNode1.getPositions()) {
                    allnum += (item.getSecond() - item.getFirst() + 1);
                }
                put.addColumn(Bytes.toBytes("std"), Bytes.toBytes("p"), indexNode1.toBytes());
                puts.add(put);
                logger.info("Put the data:{} into HBase.", put.toString());
                statisticInfo.add(new Pair<>((String) entry.getKey(), indexNode1.getPositions().size()));
            }
            table.put(puts);

            // store statistic information for query order optimization
            Collections.sort(statisticInfo, new Comparator<Pair<String, Integer>>() {
                @Override
                public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
                    return o1.getFirst().compareTo(o2.getFirst());
                }
            });

            byte[] result = new byte[Bytes.SIZEOF_INT * statisticInfo.size() * 2];
            System.arraycopy(Bytes.toBytes(Integer.parseInt(statisticInfo.get(0).getFirst())), 0, result, 0, Bytes.SIZEOF_INT);
            System.arraycopy(Bytes.toBytes(statisticInfo.get(0).getSecond()), 0, result, Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
            for (int i = 1; i < statisticInfo.size(); i++) {
                statisticInfo.get(i).setSecond(statisticInfo.get(i).getSecond() + statisticInfo.get(i - 1).getSecond());
                System.arraycopy(Bytes.toBytes(Integer.parseInt(statisticInfo.get(i).getFirst())), 0, result, 2 * i * Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
                System.arraycopy(Bytes.toBytes(statisticInfo.get(i).getSecond()), 0, result, (2 * i + 1) * Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
            }
            Put put = new Put(Bytes.toBytes(prefix + "statistic"));
            put.addColumn(Bytes.toBytes("std"), Bytes.toBytes("p"), result);
            table.put(put);
            logger.info("Put the data:{} into HBase.", put.toString());
        }

        long getStatisticSplitPoint(int amount) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(regionBegin * TimeSeriesNode.TIME_STEP);
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            calendar.add(Calendar.MONTH, amount);
            return calendar.getTimeInMillis() / TimeSeriesNode.TIME_STEP;
        }

        void run() throws IOException {
            int it = 0, ep = 0;
            long splitPoint = getStatisticSplitPoint(1), loc = 0;

            String lastMeanRound = null;
            IndexNode indexNode = null;
            Map<String, IndexNode> indexNodeMap = new HashMap<>();

            while (!done) {
                // Read first Wu-1 points
                int k;
                if (it == 0 || (ep > 0 && ep < EPOCH)) {
                    for (k = 0; k < Wu - 1; k++) {
                        if (nextData()) {
                            d = getCurrentData();
                            buffer[k] = d;
                        } else
                            break;
                    }
                } else {
                    for (k = 0; k < Wu - 1; k++) {
                        buffer[k] = buffer[EPOCH - Wu + 1 + k];
                    }
                }

                // Read buffer of size EPOCH or when all data has been read.
                ep = k;
                while (ep < EPOCH) {
                    if (nextData()) {
                        d = getCurrentData();
                        buffer[ep] = d;
                        ep++;
                    } else {
                        break;
                    }
                }

                // Data are read in chunk of size EPOCH.
                // When there is nothing to read, the loop is end.
                if (ep <= Wu - 1) {
                    missNum += ep;
                    // put the last node
                    if (indexNode != null && !indexNode.getPositions().isEmpty()) {
                        indexNodeMap.put(lastMeanRound, indexNode);
                    }

                    lastMeanRound = null;
                    indexNode = null;
                } else {
                    // Just for printing a dot for approximate a million point. Not much accurate.
//                    if (it % (1000000 / (EPOCH - Wu + 1)) == 0) {
//                        System.err.print(".");
//                    }

                    // Do main task here..
                    ex = 0;
                    ex2 = 0;
                    for (int i = 0; i < ep; i++) {
                        // A bunch of data has been read and pick one of them at a time to use
                        d = buffer[i];

                        // Calculate sum and sum square
                        ex += d;
                        ex2 += d * d;

                        // t is a circular array for keeping current data
                        t[i % Wu] = d;

                        // Double the size for avoiding using modulo "%" operator
                        t[(i % Wu) + Wu] = d;

                        // Start the task when there are more than m-1 points in the current chunk
                        if (i >= Wu - 1) {
                            mean = ex / Wu;
                            std = ex2 / Wu;
                            std = Math.sqrt(std - mean * mean);

                            // compute the start location of the data in the current circular array, t
                            int j = (i + 1) % Wu;

                            // store the mean and std for current chunk
                            loc = (it) * (EPOCH - Wu + 1) + i - Wu + 1 + 1 + first - 1 + missNum;
                            if (loc >= regionEnd) {
                                done = true;
                                break;
                            }
                            // split the statistic info
                            if (loc >= splitPoint) {

                                // put the last node
                                if (indexNode != null && !indexNode.getPositions().isEmpty()) {
                                    indexNodeMap.put(lastMeanRound, indexNode);
                                }

                                putDataIntoHbase(indexNodeMap, regionBegin);

                                regionBegin = splitPoint;
                                splitPoint = getStatisticSplitPoint(1);
                                logger.info("\n loc is {},\n first is {},\n regionBegin is {},\n splitPoint is {},\n regionEnd is {}.\n",
                                        loc, first, regionBegin, splitPoint, regionEnd);
                                lastMeanRound = null;
                                indexNode = null;
                                indexNodeMap.clear();
                            }
                            String meanLong = String.valueOf(Double.doubleToLongBits(mean));
                            String curMeanRound = meanLong.charAt(0) == '-' ? meanLong.substring(0, Ur + 1) : meanLong.substring(0, Ur);
//                            logger.debug("mean:{}({})[{}], std:{}, loc:{}", mean, meanLong, curMeanRound, std, loc);

                            if (lastMeanRound == null || !curMeanRound.equals(lastMeanRound)) {
                                // put the last row
                                if (lastMeanRound != null) {
                                    indexNodeMap.put(lastMeanRound, indexNode);
//                                    logger.info("Add an Interval:[{},{}].", lastMeanRound, indexNode);
                                }
                                // new row
//                                logger.debug("new row, rowkey: {}", curMeanRound);
                                indexNode = indexNodeMap.get(curMeanRound);
                                if (indexNode == null) {
                                    indexNode = new IndexNode();
                                }
                                indexNode.getPositions().add(new Pair<>((int) (loc - regionBegin), (int) (loc - regionBegin)));
                                lastMeanRound = curMeanRound;
                            } else {
                                // use last row
//                                logger.debug("use last row, rowkey: {}", lastMeanRound);
                                int index = indexNode.getPositions().size();
                                indexNode.getPositions().get(index - 1).setSecond((int) (loc - regionBegin));
                            }


                            // Reduce obsolete points from sum and sum square
                            ex -= t[j];
                            ex2 -= t[j] * t[j];
                        }


                    }

                    // If the size of last chunk is less then EPOCH, then no more data and terminate.
                    if (ep < EPOCH) {
                        // put the last node
                        if (indexNode != null && !indexNode.getPositions().isEmpty()) {
                            indexNodeMap.put(lastMeanRound, indexNode);
                        }
                        lastMeanRound = null;
                        indexNode = null;
                        missNum += ep;
                    } else {
                        it++;
                    }

                    missNum += tmpMissNum;
                    tmpMissNum = 0;
                }
            }

            // put the last node
            if (indexNode != null && !indexNode.getPositions().isEmpty()) {
                indexNodeMap.put(lastMeanRound, indexNode);
            }
            if (!indexNodeMap.isEmpty()) {
                putDataIntoHbase(indexNodeMap, regionBegin);
            }
            indexNodeMap.clear();

            table.close();
            logger.info("allnum {}", allnum);
        }

    }
}
