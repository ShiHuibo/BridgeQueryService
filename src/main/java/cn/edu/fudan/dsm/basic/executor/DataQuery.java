package cn.edu.fudan.dsm.basic.executor;

import cn.edu.fudan.dsm.basic.common.IndexCache;
import cn.edu.fudan.dsm.basic.common.Interval;
import cn.edu.fudan.dsm.basic.common.QuerySegment;
import cn.edu.fudan.dsm.basic.common.entity.IndexNode;
import cn.edu.fudan.dsm.basic.common.entity.IndexRowKey;
import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesRowKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by huibo on 2016/12/26.
 */
public class DataQuery {

    private static final Logger logger = LoggerFactory.getLogger(DataQuery.class);
    private Configuration confIn = HBaseConfiguration.create();
    private Configuration confOut = HBaseConfiguration.create();


    public List<Long> positionsBegin = new ArrayList<>();
    public List<Long> positionsEnd = new ArrayList<>();

    public List<Pair<Long, Double>> answers = new ArrayList<>();
    public List<Pair<Long, Long>> scandataPositions = new ArrayList<>();

    private Admin adminIn = null;
    private Admin adminOut = null;

    private List<Put> puts = new ArrayList<>();
    private byte[] familyName = Bytes.toBytes("std");
    private byte[] qualifierName = Bytes.toBytes("p");

    public DataQuery() {
        confOut.addResource("localhbase/hbase-site.xml");
        confOut.addResource("localhbase/hdfs-site.xml");
    }

    private void getAdmin() throws IOException {
        Connection connIn = ConnectionFactory.createConnection(confIn);
        Connection connOut = ConnectionFactory.createConnection(confOut);
        adminIn = connIn.getAdmin();
        adminOut = connOut.getAdmin();
    }

    private Table getTable(Admin admin, TableName tableName)
            throws IOException {
        if (!admin.tableExists(tableName)) {
            HTableDescriptor htd = new HTableDescriptor(tableName);
            HColumnDescriptor hcd = new HColumnDescriptor(familyName);
            htd.addFamily(hcd);
            admin.createTable(htd);
        }
        return admin.getConnection().getTable(tableName);
    }

    private Map<Long, TimeSeriesNode> dataCaches = new HashMap<>();
    private List<IndexCache> indexCaches = new ArrayList<>();

    private long allTimeUsage, scanTimeUsage;
    private int cntRowsScanned;
    private long regionBegin, regionEnd;
    private String zeroStr = "0000000000000000000"; //19 * '0'
    Table indextable = null;

    public void query(String channelCode, int Wu, int Ur, double Epsilon, List<Double> means, boolean useCache) throws IOException {
        getAdmin();

        zeroStr = zeroStr.substring(Ur);
        allTimeUsage = -System.currentTimeMillis();
        scanTimeUsage = 0;
        cntRowsScanned = 0;
        indexCaches = new ArrayList<>();  // only use it for this time

        boolean isLast = false;
        // preparation
        indextable = getTable(adminOut, TableName.valueOf("tmpindextable"));

        regionBegin = 1175356800000l;
        regionEnd = 1177948800000l;
        String prefix = regionBegin + "_" + channelCode + "_";
        regionBegin = regionBegin / TimeSeriesNode.TIME_STEP;
        regionEnd = regionEnd / TimeSeriesNode.TIME_STEP;

        logger.info("Prefix : {}, regionBegin : {}, regionEnd : {}.", prefix, regionBegin, regionEnd);

        // optimize query order
        List<QuerySegment> queries = optimizeQueryOrder(Wu, Ur, Epsilon, means, prefix);
        logger.info("{} - Query order: {}", prefix, queries);

        // do query
        List<Interval> validPositions = new ArrayList<>();
        int cntLastCandidate = 0;
        List<Integer> cntCandidates = new ArrayList<>(means.size());

        int lastSegment = queries.get(queries.size() - 1).getOrder();
        double lastMinimumEpsilon = 0;
        double range0 = Epsilon * Epsilon / Wu;
        for (int i = 0; i < queries.size(); i++) {
            QuerySegment query = queries.get(i);

            logger.info("{} - Disjoint window #{} - {} - mean:{}", prefix, i + 1, query.getOrder(), query.getMean());

            int deltaW = (i == queries.size() - 1) ? 0 : (queries.get(i + 1).getOrder() - query.getOrder()) * Wu;

            List<Interval> nextValidPositions = new ArrayList<>();

            // store possible current segment
            List<Interval> positions = new ArrayList<>();

            // query possible rows which mean is in possible distance range of i th disjoint window
            double range = Math.sqrt(range0 - lastMinimumEpsilon);
            String begin = String.valueOf(Double.doubleToLongBits(query.getMean() - range));
            String beginRound = begin.charAt(0) == '-' ? begin.substring(0, Ur + 1) : begin.substring(0, Ur);
            String end = String.valueOf(Double.doubleToLongBits(query.getMean() + range));
            String endRound = end.charAt(0) == '-' ? end.substring(0, Ur + 1) : end.substring(0, Ur);
//                logger.debug("{} - Scan index [{}, {})", prefix, prefix + query.getBeginRound(), prefix + query.getEndRound());

            if (useCache) {
                int index_l = findCache(beginRound);
                int index_r = findCache(endRound, index_l);
                logger.info("{} - [{}, {}] ({}, {})", prefix, beginRound, endRound, index_l, index_r);
                if (index_l == index_r && index_l >= 0) {
                    /**
                     * Current:          l|===|r
                     * Cache  : index_l_l|_____|index_l_r
                     * Future : index_l_l|_____|index_l_r
                     */
                    scanCache(index_l, beginRound, true, endRound, true, query, positions);
                } else if (index_l < 0 && index_r >= 0) {
                    /**
                     * Current:         l|_==|r
                     * Cache  :   index_r_l|_____|index_r_r
                     * Future : index_r_l|_______|index_r_r
                     */
                    scanCache(index_r, indexCaches.get(index_r).getBeginRound(), true, endRound, true, query, positions);
                    scanHBaseAndAddCache(prefix, beginRound, true, indexCaches.get(index_r).getBeginRound(), false, index_r, query, positions);
                    indexCaches.get(index_r).setBeginRound(beginRound);
                } else if (index_l >= 0 && index_r < 0) {
                    /**
                     * Current:             l|==_|r
                     * Cache  : index_l_l|_____|index_l_r
                     * Future : index_l_l|_______|index_l_r
                     */
                    scanCache(index_l, beginRound, true, indexCaches.get(index_l).getEndRound(), true, query, positions);
                    scanHBaseAndAddCache(prefix, indexCaches.get(index_l).getEndRound(), false, endRound, true, index_l, query, positions);
                    indexCaches.get(index_l).setEndRound(endRound);
                } else if (index_l == index_r && index_l < 0) {
                    /**
                     * Current:        l|___|r
                     * Cache  : |_____|       |_____|
                     * Future : |_____|l|___|r|_____|
                     */
                    scanHBaseAndAddCache(prefix, beginRound, true, endRound, true, index_r, query, positions);  // insert a new cache node
                } else if (index_l >= 0 && index_r >= 0 && index_l + 1 == index_r) {
                    /**
                     * Current:     l|=___=|r
                     * Cache  : |_____|s  |_____|
                     * Future : |_______________|
                     */
                    String s = indexCaches.get(index_l).getEndRound();
                    scanCache(index_l, beginRound, true, s, true, query, positions);
                    scanHBaseAndAddCache(prefix, s, false, indexCaches.get(index_r).getBeginRound(), false, index_r, query, positions);
                    scanCache(index_r, indexCaches.get(index_r).getBeginRound(), true, endRound, true, query, positions);
                    indexCaches.get(index_r).setBeginRound(s + "1");
                }
            } else {
                scanHBase(prefix, beginRound, true, endRound, true, query, positions);
            }
            positions = sortButNotMergeIntervals(positions, Epsilon);
//                logger.info("{} - position: {}", prefix, positions.toString());

            lastMinimumEpsilon = Double.MAX_VALUE;

            if (i == 0) {
                for (Interval position : positions) {
                    nextValidPositions.add(new Interval(position.getLeft() + deltaW, position.getRight() + deltaW, position.getEpsilon()));

                    if (position.getEpsilon() < lastMinimumEpsilon) {
                        lastMinimumEpsilon = position.getEpsilon();
                    }
                }
                if (query.getOrder() > 1 && !isLast) {
                    nextValidPositions.add(new Interval(regionEnd, regionEnd - 1 + (query.getOrder() - 1) * Wu, 0));
                    lastMinimumEpsilon = 0;
                }
            } else {
                int index1 = 0, index2 = 0;  // 1 - validPositions, 2-positions

                for (int index = 0; index < validPositions.size(); index++) {
                    if (validPositions.get(index).getLeft() < regionBegin) {
                        if (validPositions.get(index).getRight() >= regionBegin) {
                            nextValidPositions.add(new Interval(validPositions.get(index).getLeft() + deltaW, regionBegin - 1 + deltaW, validPositions.get(index).getEpsilon()));
                            validPositions.get(index).setLeft(regionBegin);
                            break;
                        } else {
                            nextValidPositions.add(new Interval(validPositions.get(index).getLeft() + deltaW, validPositions.get(index).getRight() + deltaW, validPositions.get(index).getEpsilon()));
                            if (validPositions.get(index).getEpsilon() < lastMinimumEpsilon) {
                                lastMinimumEpsilon = validPositions.get(index).getEpsilon();
                            }
                            index1 = index + 1;  // not remove actually to optimize performance
                        }
                    } else {
                        break;  // no border positions because it has been sorted
                    }
                }

                for (int index = validPositions.size() - 1; index >= 0; index--) {
                    if (validPositions.get(index).getRight() >= regionEnd) {
                        if (validPositions.get(index).getLeft() < regionEnd) {
                            nextValidPositions.add(new Interval(regionEnd + deltaW, validPositions.get(index).getRight() + deltaW, validPositions.get(index).getEpsilon()));
                            validPositions.get(index).setRight(regionEnd - 1);
                            break;  // no other positions because it has been sorted
                        } else {
                            nextValidPositions.add(new Interval(validPositions.get(index).getLeft() + deltaW, validPositions.get(index).getRight() + deltaW, validPositions.get(index).getEpsilon()));
                            if (validPositions.get(index).getEpsilon() < lastMinimumEpsilon) {
                                lastMinimumEpsilon = validPositions.get(index).getEpsilon();
                            }
                            validPositions.remove(index);
                        }
                    } else {
                        break;  // no border positions because it has been sorted
                    }
                }

                while (index1 < validPositions.size() && index2 < positions.size()) {
                    if (validPositions.get(index1).getRight() < positions.get(index2).getLeft()) {
                        index1++;
                    } else if (positions.get(index2).getRight() < validPositions.get(index1).getLeft()) {
                        index2++;
                    } else {
                        double sumEpsilon = validPositions.get(index1).getEpsilon() + positions.get(index2).getEpsilon();
                        if (validPositions.get(index1).getRight() < positions.get(index2).getRight()) {
                            if (sumEpsilon <= Epsilon * Epsilon / Wu) {
                                nextValidPositions.add(new Interval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, validPositions.get(index1).getRight() + deltaW, sumEpsilon));
                                if (sumEpsilon < lastMinimumEpsilon) {
                                    lastMinimumEpsilon = sumEpsilon;
                                }
                            }
                            index1++;
                        } else {
                            if (sumEpsilon <= Epsilon * Epsilon / Wu) {
                                nextValidPositions.add(new Interval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, positions.get(index2).getRight() + deltaW, sumEpsilon));
                                if (sumEpsilon < lastMinimumEpsilon) {
                                    lastMinimumEpsilon = sumEpsilon;
                                }
                            }
                            index2++;
                        }
                    }
                }
            }

            validPositions = sortButNotMergeIntervals(nextValidPositions, Epsilon);
//                logger.info("{} - next valid: {}", prefix, validPositions.toString());

            int cntCurrentCandidate = validPositions.size();  // count for consecutive positions
//                for (Interval validPosition : validPositions) {  // count for different valid begin offsets
//                    cntCurrentCandidate += validPosition.getRight() - validPosition.getLeft() + 1;
//                }

            cntCandidates.add(cntCurrentCandidate);
            logger.info("{} - Candidate: {}", prefix, cntCurrentCandidate);
            if (cntCurrentCandidate == 0 || (cntLastCandidate != 0 && cntCurrentCandidate < 100 && (double) cntCurrentCandidate / cntLastCandidate >= 0.8)) {
                lastSegment = (i == queries.size() - 1) ? query.getOrder() : queries.get(i + 1).getOrder();
                break;
            }
            cntLastCandidate = cntCurrentCandidate;
        }

        // merge consecutive intervals to shrink data size and alleviate scan times
        validPositions = sortAndMergeIntervals(validPositions);

        for (Interval validPosition : validPositions) {
            positionsBegin.add(validPosition.getLeft() - (lastSegment - 1) * Wu);
            positionsEnd.add(validPosition.getRight() - (lastSegment - 1) * Wu);
        }
        cntCandidates.add(cntRowsScanned);

        allTimeUsage += System.currentTimeMillis();
        logger.info("{} - Time usage: all - {} ms, scan - {} ms", prefix, allTimeUsage, scanTimeUsage);

        indexCaches.clear();

    }

    private void scanHBase(String prefix, String begin, boolean beginInclusive, String end, boolean endInclusive, QuerySegment query, List<Interval> positions) throws IOException {
        if (!beginInclusive) begin = begin + "1";
        if (endInclusive) end = end + "1";

        Scan scan = new Scan(Bytes.toBytes(prefix + begin), Bytes.toBytes(prefix + end));
        try (ResultScanner scanner = indextable.getScanner(scan)) {
            for (Result r : scanner) {

                if (!r.isEmpty()) {
                    cntRowsScanned++;

                    IndexNode indexNode = new IndexNode();
                    indexNode.parseBytes(r.getValue(familyName, qualifierName));

                    String stringMeanRound = new IndexRowKey(Bytes.toString(r.getRow())).getValue();
                    double lowerBound = getDistanceLowerBound(query, stringMeanRound);
                    for (Pair<Integer, Integer> position : indexNode.getPositions()) {
                        positions.add(new Interval(position.getFirst() + regionBegin - 1, position.getSecond() + regionBegin - 1, lowerBound));
                    }
                }
            }
        }
    }

    private void scanHBaseAndAddCache(String prefix, String begin, boolean beginInclusive, String end, boolean endInclusive, int index, QuerySegment query, List<Interval> positions) throws IOException {
        if (index < 0) {
            index = -index - 1;
            indexCaches.add(index, new IndexCache(begin, end));
        }

        if (!beginInclusive) begin = begin + "1";
        if (endInclusive) end = end + "1";

        Scan scan = new Scan(Bytes.toBytes(prefix + begin), Bytes.toBytes(prefix + end));
        try (ResultScanner scanner = indextable.getScanner(scan)) {
            for (Result r : scanner) {

                if (!r.isEmpty()) {
                    cntRowsScanned++;

                    IndexNode indexNode = new IndexNode();
                    indexNode.parseBytes(r.getValue(familyName, qualifierName));

                    String stringMeanRound = new IndexRowKey(Bytes.toString(r.getRow())).getValue();
                    double lowerBound = getDistanceLowerBound(query, stringMeanRound);
                    for (Pair<Integer, Integer> position : indexNode.getPositions()) {
                        positions.add(new Interval(position.getFirst() + regionBegin, position.getSecond() + regionBegin, lowerBound));
                    }

                    indexCaches.get(index).addCache(stringMeanRound, indexNode);
                }
            }
        }
    }

    private void scanCache(int index, String begin, boolean beginInclusive, String end, boolean endInclusive, QuerySegment query, List<Interval> positions) {
        for (Map.Entry entry : indexCaches.get(index).getCaches().subMap(begin, beginInclusive, end, endInclusive).entrySet()) {
            String stringMeanRound = (String) entry.getKey();
            IndexNode indexNode = (IndexNode) entry.getValue();
            double lowerBound = getDistanceLowerBound(query, stringMeanRound);
            for (Pair<Integer, Integer> position : indexNode.getPositions()) {
                positions.add(new Interval(position.getFirst() + regionBegin - 1, position.getSecond() + regionBegin - 1, lowerBound));
            }
        }
    }

    private int findCache(String round) {
        return findCache(round, 0);
    }

    private int findCache(String round, int first) {
        if (first < 0) {
            first = -first - 1;
        }

        for (int i = first; i < indexCaches.size(); i++) {
            IndexCache cache = indexCaches.get(i);
            if (cache.getBeginRound().compareTo(round) > 0) {
                return -i - 1;
            }
            if (cache.getBeginRound().compareTo(round) <= 0 && cache.getEndRound().compareTo(round) >= 0) {
                return i;
            }
        }
        return -1;
    }

    public int scanData(String channelCode, int Wr, int M, double Epsilon, List<Float> query) {
        try {
            // get basic region information
            long regionBegin, regionEnd;
            regionBegin = 0;  // scan data between 1970.01.01 and now
            regionEnd = System.currentTimeMillis() / TimeSeriesNode.TIME_STEP;

//            BridgeQueryProtos.ScanDataResponse.Builder responseBuilder = BridgeQueryProtos.ScanDataResponse.newBuilder();

            // judge whether position is in the region range, within - calculate, intersection - send back directly, no - ignore
            int cntCandidate = 0;
            for (int index = 0; index < positionsBegin.size(); index++) {
                cntCandidate += positionsEnd.get(index) - positionsBegin.get(index) + 1;

                long left = positionsBegin.get(index);
                long right = positionsEnd.get(index) + M - 1;

                long begin = left / Wr * Wr;
                long end = right / Wr * Wr;

                if (end < regionBegin || begin >= regionEnd) {
                    // ignore, do noting in this region
                } else if (begin >= regionBegin && end < regionEnd) {
                    // within
                    logger.debug("Scan data [{}, {}]", left, right);
                    List<Float> data = getTimeSeries(channelCode, left, right - left + 1, begin, end, Wr);

                    for (int i = 0; i + M - 1 < data.size(); i++) {
                        double distance = 0;
                        for (int j = 0; j < M && distance <= Epsilon * Epsilon; j++) {
                            distance += (data.get(i + j) - query.get(j)) * (data.get(i + j) - query.get(j));
                        }
                        if (distance <= Epsilon * Epsilon) {
                            answers.add(new Pair<>(left + i, Math.sqrt(distance)));
                        }
                    }
                } else if (begin < regionEnd && regionEnd <= end) {  // avoid duplicate with other regions
                    // intersection, leave for client to scan data
                    logger.debug("Leave for client [{}, {}]", left, right);
                    scandataPositions.add(new Pair<>(positionsBegin.get(index), positionsEnd.get(index)));
                }
            }

            return cntCandidate;
        } catch (IOException ioe) {
            logger.error(ioe.getMessage(), ioe.getCause());
        }
        return 0;
    }

    private List<Float> getTimeSeries(String channelCode, long left, long length, long begin, long end, int Wr) throws IOException {
        List<Float> ret = new ArrayList<>();
        Table histtable = getTable(adminOut, TableName.valueOf("histtable"));

        if (begin == end) {  // `get` is faster
            TimeSeriesNode node = dataCaches.get(begin);  // try to use dataCaches first
            if (node == null) {
                Get get = new Get(Bytes.toBytes(new TimeSeriesRowKey(channelCode, (begin-1) * TimeSeriesNode.TIME_STEP).toString()));
                get.addColumn(Bytes.toBytes("value"), Bytes.toBytes("data"));
                Result result = histtable.get(get);

                node = new TimeSeriesNode();
                node.parseBytes(begin * TimeSeriesNode.TIME_STEP, result.getValue(Bytes.toBytes("value"), Bytes.toBytes("data")));

                dataCaches.clear();
                dataCaches.put(begin, node);
            }
            int offset = (int) ((left - 1) % Wr);
            List<Pair<Long, Float>> nodeData = node.getData();
            int so = nodeData.get(0).getFirst().intValue();
            int eo = nodeData.size();
            for (int i = offset < so ? so : offset; i < Math.min(offset + length, eo); i++) {
                ret.add(nodeData.get(i).getSecond());
            }
        } else {  // use `scan` instead
            Scan scan = new Scan(Bytes.toBytes(new TimeSeriesRowKey(channelCode, begin * TimeSeriesNode.TIME_STEP).toString()),
                    Bytes.toBytes(new TimeSeriesRowKey(channelCode, (end+1)  * TimeSeriesNode.TIME_STEP).toString()));
            scan.addColumn(Bytes.toBytes("value"), Bytes.toBytes("data"));

            try (ResultScanner scanner = histtable.getScanner(scan)) {
                Pair<Long, Float> lastNode = null;
                long lastOffset = 0;
                for (Result r : scanner) {

                    TimeSeriesNode node = new TimeSeriesNode();
                    TimeSeriesRowKey tmpRowKey = new TimeSeriesRowKey(r.getRow());
                    node.parseBytes(tmpRowKey.getFirst(), r.getValue(Bytes.toBytes("value"), Bytes.toBytes("data")));

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
                        dataCaches.clear();
                        dataCaches.put(first, node);

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


            }
        }
        if (ret.size() > length) {
            throw new IOException("data fetch mistake!");
        }
        return ret;
    }

    private double getDistanceLowerBound(QuerySegment query, String stringMeanRound) {
        long longValue = Long.parseLong(stringMeanRound);
        String meanLowerStr = stringMeanRound + zeroStr;  // TODO: Ur=6
        double meanLower = Double.longBitsToDouble(Long.valueOf(meanLowerStr));
        String meanUpperStr = (longValue < 0 ? String.valueOf(longValue - 1) : String.valueOf(longValue + 1)) + zeroStr;
        double meanUpper = Double.longBitsToDouble(Long.valueOf(meanUpperStr));

        double delta;
        if (meanLower > query.getMean()) {
            delta = (meanLower - query.getMean()) * (meanLower - query.getMean());
        } else if (meanUpper < query.getMean()) {
            delta = (query.getMean() - meanUpper) * (query.getMean() - meanUpper);
        } else {
            delta = 0;
        }

        return delta;
    }

    private List<QuerySegment> optimizeQueryOrder(int Wu, int Ur, double Epsilon, List<Double> means, String prefix) throws IOException {
        List<QuerySegment> queries = new ArrayList<>(means.size());

        // get statistic information
        Table indextable = getTable(adminOut, TableName.valueOf("tmpindextable"));
        byte[] statisticData = indextable.get(new Get(Bytes.toBytes(prefix + "statistic"))).getValue(familyName, qualifierName);
        List<Pair<String, Integer>> statisticInfo = new ArrayList<>(statisticData.length / Bytes.SIZEOF_INT);
        byte[] tmp = new byte[Bytes.SIZEOF_INT];
        for (int i = 0; i < statisticData.length; i += 2 * Bytes.SIZEOF_INT) {
            System.arraycopy(statisticData, i, tmp, 0, Bytes.SIZEOF_INT);
            String key = String.valueOf(Bytes.toInt(tmp));
            System.arraycopy(statisticData, i + Bytes.SIZEOF_INT, tmp, 0, Bytes.SIZEOF_INT);
            int value = Bytes.toInt(tmp);
            statisticInfo.add(new Pair<>(key, value));
        }

        // optimize query order
        double range = Epsilon / Math.sqrt(Wu);
        for (int i = 0; i < means.size(); i++) {
            double meanQ = means.get(i);

            String begin = String.valueOf(Double.doubleToLongBits(meanQ - range));
            String beginRound = (begin.charAt(0) == '-' ? begin.substring(0, Ur + 1) : begin.substring(0, Ur));
            String end = String.valueOf(Double.doubleToLongBits(meanQ + range));
            String endRound = ((end.charAt(0) == '-' ? end.substring(0, Ur + 1) : end.substring(0, Ur)) + "1");

            int index = Collections.binarySearch(statisticInfo, new Pair<>(beginRound, 0), new Comparator<Pair<String, Integer>>() {
                @Override
                public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
                    return o1.getFirst().compareTo(o2.getFirst());
                }
            });
            index = index < 0 ? -(index + 1) : index;
            int lower = index > 0 ? statisticInfo.get(index - 1).getSecond() : 0;

            index = Collections.binarySearch(statisticInfo, new Pair<>(endRound, 0), new Comparator<Pair<String, Integer>>() {
                @Override
                public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
                    return o1.getFirst().compareTo(o2.getFirst());
                }
            });
            index = index < 0 ? -(index + 1) : index;
            int upper = index > 0 ? statisticInfo.get(index - 1).getSecond() : 0;

            queries.add(new QuerySegment(meanQ, beginRound, endRound, i + 1, upper - lower));
        }

        Collections.sort(queries, new Comparator<QuerySegment>() {
            @Override
            public int compare(QuerySegment o1, QuerySegment o2) {
                return o1.getCount() - o2.getCount();
            }
        });

        return queries;
    }

    private List<Interval> sortButNotMergeIntervals(List<Interval> intervals, double Epsilon) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        Collections.sort(intervals, new Comparator<Interval>() {
            @Override
            public int compare(Interval o1, Interval o2) {
                return Long.compare(o1.getLeft(), o2.getLeft());
            }
        });

        Interval first = intervals.get(0);
        long start = first.getLeft();
        long end = first.getRight();
        double epsilon = first.getEpsilon();

        List<Interval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            Interval current = intervals.get(i);
            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Math.abs(current.getEpsilon() - epsilon) < 1)) {
                end = Math.max(current.getRight(), end);
                epsilon = Math.min(current.getEpsilon(), epsilon);
            } else {
                result.add(new Interval(start, end, epsilon));
                start = current.getLeft();
                end = current.getRight();
                epsilon = current.getEpsilon();
            }
        }
        result.add(new Interval(start, end, epsilon));

        return result;
    }

    private List<Interval> sortAndMergeIntervals(List<Interval> intervals) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        Collections.sort(intervals, new Comparator<Interval>() {
            @Override
            public int compare(Interval o1, Interval o2) {
                return Long.compare(o1.getLeft(), o2.getLeft());
            }
        });

        Interval first = intervals.get(0);
        long start = first.getLeft();
        long end = first.getRight();
        double epsilon = first.getEpsilon();

        List<Interval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            Interval current = intervals.get(i);
            if (current.getLeft() - 1 <= end) {
                end = Math.max(current.getRight(), end);
                epsilon = Math.min(current.getEpsilon(), epsilon);
            } else {
                result.add(new Interval(start, end, epsilon));
                start = current.getLeft();
                end = current.getRight();
                epsilon = current.getEpsilon();
            }
        }
        result.add(new Interval(start, end, epsilon));

        return result;
    }
}
