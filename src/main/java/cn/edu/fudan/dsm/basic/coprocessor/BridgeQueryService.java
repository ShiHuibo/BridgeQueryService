package cn.edu.fudan.dsm.basic.coprocessor;

import cn.edu.fudan.dsm.basic.common.IndexCache;
import cn.edu.fudan.dsm.basic.common.Interval;
import cn.edu.fudan.dsm.basic.common.QuerySegment;
import cn.edu.fudan.dsm.basic.common.entity.IndexNode;
import cn.edu.fudan.dsm.basic.common.entity.IndexRowKey;
import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesRowKey;
import cn.edu.fudan.dsm.basic.coprocessor.generated.BridgeQueryProtos;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by huibo on 2016/12/7.
 */
public class BridgeQueryService extends BridgeQueryProtos.BridgeQueryService implements Coprocessor, CoprocessorService {

    private static final Logger logger = LoggerFactory.getLogger(BridgeQueryService.class.getName());

    private RegionCoprocessorEnvironment env;

    private Map<Long, TimeSeriesNode> dataCaches = new HashMap<>();

    private List<IndexCache> indexCaches = new ArrayList<>();

    private long allTimeUsage, scanTimeUsage;
    private int cntRowsScanned;
    private long regionBegin, regionEnd;

    @Override
    public void buildIndex(RpcController controller, BridgeQueryProtos.BuildIndexRequest request, RpcCallback<BridgeQueryProtos.BuildIndexResponse> done) {
        try {
            Scan tmpScan = new Scan()
                    .setStopRow(Bytes.toBytes(request.getChannelCode()))
                    .setStartRow(Bytes.toBytes(request.getChannelCode() + "_" + Long.MAX_VALUE))
                    .setCaching(1)
                    .setReversed(true);
            try (InternalScanner tmpScanner = env.getRegion().getScanner(tmpScan)) {
                // scan data between 2000.01.01 and now
                List<Cell> endRowKey = new ArrayList<>();
                tmpScanner.next(endRowKey);
                regionEnd = new TimeSeriesRowKey(Bytes.toString(CellUtil.cloneRow(endRowKey.get(0)))).getFirst();
                tmpScanner.close();
            }
            tmpScan = new Scan();
            tmpScan.setCaching(1);
            List<Cell> tmpRowKey = new ArrayList<>();
            env.getRegion().getScanner(tmpScan).next(tmpRowKey);
            regionBegin = new TimeSeriesRowKey(Bytes.toString(CellUtil.cloneRow(tmpRowKey.get(0)))).getFirst();
            Calendar calendar = Calendar.getInstance();
            if (!Arrays.equals(env.getRegion().getRegionInfo().getStartKey(), HConstants.EMPTY_BYTE_ARRAY)) {
                calendar.setTimeInMillis(regionBegin);
                calendar.set(Calendar.DAY_OF_MONTH, 1);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                calendar.add(Calendar.MONTH, 1);
                regionBegin = calendar.getTimeInMillis();
            }
            if (!Arrays.equals(env.getRegion().getRegionInfo().getEndKey(), HConstants.EMPTY_BYTE_ARRAY)) {
                calendar.setTimeInMillis(regionEnd);
                calendar.set(Calendar.DAY_OF_MONTH, 1);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                regionEnd = calendar.getTimeInMillis();
            }
            logger.info("regionBegin is {}, regionEnd is {}", regionBegin, regionEnd);
            Scan scan = new Scan()
                    .setStartRow(Bytes.toBytes(request.getChannelCode() + "_" + regionBegin))
                    .setStopRow(Bytes.toBytes(request.getChannelCode() + "_" + regionEnd))
                    .setCaching(500);
            try (InternalScanner scanner = env.getRegion().getScanner(scan)) {
                IndexBuilder indexBuilder = new IndexBuilder(request.getChannelCode(), request.getWr(), request.getWu(), request.getUr(), regionBegin, regionEnd, scanner, env);
                indexBuilder.run();
            }
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        }
        BridgeQueryProtos.BuildIndexResponse response = BridgeQueryProtos.BuildIndexResponse.newBuilder().setSuccess(true).build();
        done.run(response);
    }

    @Override
    public void query(RpcController controller, BridgeQueryProtos.QueryRequest request, RpcCallback<BridgeQueryProtos.QueryResponse> done) {
        try {
            String zeroStr = "0000000000000000000"; //19 * '0'
            zeroStr = zeroStr.substring(request.getUr());
            allTimeUsage = -System.currentTimeMillis();
            scanTimeUsage = 0;
            cntRowsScanned = 0;
            indexCaches = new ArrayList<>();  // only use it for this time

            boolean isLast = false;
            // preparation
            Scan scan = new Scan();
            scan.setCaching(1);
            List<Cell> tmpRowKey = new ArrayList<>();
            if (Arrays.equals(env.getRegion().getRegionInfo().getStartKey(), HConstants.EMPTY_BYTE_ARRAY)) {  // first region
                env.getRegion().getScanner(scan).next(tmpRowKey);
                if (tmpRowKey.isEmpty()) return;
                regionBegin = new IndexRowKey(Bytes.toString(CellUtil.cloneRow(tmpRowKey.get(0)))).getFirst();
            } else {
                regionBegin = Long.parseLong(Bytes.toString(env.getRegion().getRegionInfo().getStartKey()).split("_")[0]);
            }
            if (Arrays.equals(env.getRegion().getRegionInfo().getEndKey(), HConstants.EMPTY_BYTE_ARRAY)) {  // last region
                Calendar tmpC = Calendar.getInstance();
                tmpC.setTimeInMillis(regionBegin);
                tmpC.add(Calendar.MONTH, 1);
                regionEnd = tmpC.getTimeInMillis();
                isLast = true;
            } else {
                regionEnd = Long.parseLong(Bytes.toString(env.getRegion().getRegionInfo().getEndKey()).split("_")[0]);
            }
            String prefix = regionBegin + "_" + request.getChannelCode() + "_";
            regionBegin = regionBegin / TimeSeriesNode.TIME_STEP;
            regionEnd = regionEnd / TimeSeriesNode.TIME_STEP;

            logger.info("Prefix : {}, regionBegin : {}, regionEnd : {}.", prefix, regionBegin, regionEnd);

            // optimize query order
            List<QuerySegment> queries = optimizeQueryOrder(request, prefix);
            logger.info("{} - Query order: {}", prefix, queries);

            // do query
            List<Interval> validPositions = new ArrayList<>();
            int cntLastCandidate = 0;
            List<Integer> cntCandidates = new ArrayList<>(request.getMeansCount());

            int lastSegment = queries.get(queries.size() - 1).getOrder();
            double lastMinimumEpsilon = 0;
            double range0 = request.getEpsilon() * request.getEpsilon() / request.getWu();
            for (int i = 0; i < queries.size(); i++) {
                QuerySegment query = queries.get(i);

                logger.info("{} - Disjoint window #{} - {} - mean:{}", prefix, i + 1, query.getOrder(), query.getMean());

                int deltaW = (i == queries.size() - 1) ? 0 : (queries.get(i + 1).getOrder() - query.getOrder()) * request.getWu();

                List<Interval> nextValidPositions = new ArrayList<>();

                // store possible current segment
                List<Interval> positions = new ArrayList<>();

                // query possible rows which mean is in possible distance range of i th disjoint window
                double range = Math.sqrt(range0 - lastMinimumEpsilon);
                String begin = String.valueOf(Double.doubleToLongBits(query.getMean() - range));
                String beginRound = begin.charAt(0) == '-' ? begin.substring(0, request.getUr() + 1) : begin.substring(0, request.getUr());
                String end = String.valueOf(Double.doubleToLongBits(query.getMean() + range));
                String endRound = end.charAt(0) == '-' ? end.substring(0, request.getUr() + 1) : end.substring(0, request.getUr());
//                logger.debug("{} - Scan index [{}, {})", prefix, prefix + query.getBeginRound(), prefix + query.getEndRound());

                if (request.getUseCache()) {
                    int index_l = findCache(beginRound);
                    int index_r = findCache(endRound, index_l);
                    logger.info("{} - [{}, {}] ({}, {})", prefix, beginRound, endRound, index_l, index_r);
                    if (index_l == index_r && index_l >= 0) {
                        /**
                         * Current:          l|===|r
                         * Cache  : index_l_l|_____|index_l_r
                         * Future : index_l_l|_____|index_l_r
                         */
                        scanCache(index_l, beginRound, true, endRound, true, query, positions, zeroStr);
                    } else if (index_l < 0 && index_r >= 0) {
                        /**
                         * Current:         l|_==|r
                         * Cache  :   index_r_l|_____|index_r_r
                         * Future : index_r_l|_______|index_r_r
                         */
                        scanCache(index_r, indexCaches.get(index_r).getBeginRound(), true, endRound, true, query, positions, zeroStr);
                        scanHBaseAndAddCache(prefix, beginRound, true, indexCaches.get(index_r).getBeginRound(), false, index_r, query, positions, zeroStr);
                        indexCaches.get(index_r).setBeginRound(beginRound);
                    } else if (index_l >= 0 && index_r < 0) {
                        /**
                         * Current:             l|==_|r
                         * Cache  : index_l_l|_____|index_l_r
                         * Future : index_l_l|_______|index_l_r
                         */
                        scanCache(index_l, beginRound, true, indexCaches.get(index_l).getEndRound(), true, query, positions, zeroStr);
                        scanHBaseAndAddCache(prefix, indexCaches.get(index_l).getEndRound(), false, endRound, true, index_l, query, positions, zeroStr);
                        indexCaches.get(index_l).setEndRound(endRound);
                    } else if (index_l == index_r && index_l < 0) {
                        /**
                         * Current:        l|___|r
                         * Cache  : |_____|       |_____|
                         * Future : |_____|l|___|r|_____|
                         */
                        scanHBaseAndAddCache(prefix, beginRound, true, endRound, true, index_r, query, positions, zeroStr);  // insert a new cache node
                    } else if (index_l >= 0 && index_r >= 0 && index_l + 1 == index_r) {
                        /**
                         * Current:     l|=___=|r
                         * Cache  : |_____|s  |_____|
                         * Future : |_______________|
                         */
                        String s = indexCaches.get(index_l).getEndRound();
                        scanCache(index_l, beginRound, true, s, true, query, positions, zeroStr);
                        scanHBaseAndAddCache(prefix, s, false, indexCaches.get(index_r).getBeginRound(), false, index_r, query, positions, zeroStr);
                        scanCache(index_r, indexCaches.get(index_r).getBeginRound(), true, endRound, true, query, positions, zeroStr);
                        indexCaches.get(index_r).setBeginRound(s + "1");
                    }
                } else {
                    scanHBase(prefix, beginRound, true, endRound, true, query, positions, zeroStr);
                }
                positions = sortButNotMergeIntervals(positions, request.getEpsilon());
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
                        nextValidPositions.add(new Interval(regionEnd, regionEnd - 1 + (query.getOrder() - 1) * request.getWu(), 0));
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
                                if (sumEpsilon <= request.getEpsilon() * request.getEpsilon() / request.getWu()) {
                                    nextValidPositions.add(new Interval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, validPositions.get(index1).getRight() + deltaW, sumEpsilon));
                                    if (sumEpsilon < lastMinimumEpsilon) {
                                        lastMinimumEpsilon = sumEpsilon;
                                    }
                                }
                                index1++;
                            } else {
                                if (sumEpsilon <= request.getEpsilon() * request.getEpsilon() / request.getWu()) {
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

                validPositions = sortButNotMergeIntervals(nextValidPositions, request.getEpsilon());
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

            BridgeQueryProtos.QueryResponse.Builder responseBuilder = BridgeQueryProtos.QueryResponse.newBuilder();
            for (Interval validPosition : validPositions) {
                responseBuilder.addPositionsBegin(validPosition.getLeft() - (lastSegment - 1) * request.getWu());
                responseBuilder.addPositionsEnd(validPosition.getRight() - (lastSegment - 1) * request.getWu());
            }
            cntCandidates.add(cntRowsScanned);
            responseBuilder.addAllCntCandidates(cntCandidates);
            BridgeQueryProtos.QueryResponse response = responseBuilder.build();
            done.run(response);
            allTimeUsage += System.currentTimeMillis();
            logger.info("{} - Time usage: all - {} ms, scan - {} ms", prefix, allTimeUsage, scanTimeUsage);

            indexCaches.clear();
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        }
    }

    private void scanHBase(String prefix, String begin, boolean beginInclusive, String end, boolean endInclusive, QuerySegment query, List<Interval> positions, String zeroStr) throws IOException {
        if (!beginInclusive) begin = begin + "1";
        if (endInclusive) end = end + "1";

        Scan scan = new Scan(Bytes.toBytes(prefix + begin), Bytes.toBytes(prefix + end));
        try (InternalScanner scanner = env.getRegion().getScanner(scan)) {
            boolean finished = false;
            while (!finished) {
                List<Cell> results = new ArrayList<>();
                finished = !scanner.next(results);

                if (!results.isEmpty()) {
                    cntRowsScanned++;

                    IndexNode indexNode = new IndexNode();
                    indexNode.parseBytes(CellUtil.cloneValue(results.get(0)));

                    String stringMeanRound = new IndexRowKey(Bytes.toString(CellUtil.cloneRow(results.get(0)))).getValue();
                    double lowerBound = getDistanceLowerBound(query, stringMeanRound, zeroStr);
                    for (Pair<Integer, Integer> position : indexNode.getPositions()) {
                        positions.add(new Interval(position.getFirst() + regionBegin - 1, position.getSecond() + regionBegin - 1, lowerBound));
                    }
                }
            }
        }
    }

    private void scanHBaseAndAddCache(String prefix, String begin, boolean beginInclusive, String end, boolean endInclusive, int index, QuerySegment query, List<Interval> positions, String zeroStr) throws IOException {
        if (index < 0) {
            index = -index - 1;
            indexCaches.add(index, new IndexCache(begin, end));
        }

        if (!beginInclusive) begin = begin + "1";
        if (endInclusive) end = end + "1";

        Scan scan = new Scan(Bytes.toBytes(prefix + begin), Bytes.toBytes(prefix + end));
        try (InternalScanner scanner = env.getRegion().getScanner(scan)) {
            boolean finished = false;
            while (!finished) {
                List<Cell> results = new ArrayList<>();
                finished = !scanner.next(results);

                if (!results.isEmpty()) {
                    cntRowsScanned++;

                    IndexNode indexNode = new IndexNode();
                    indexNode.parseBytes(CellUtil.cloneValue(results.get(0)));

                    String stringMeanRound = new IndexRowKey(Bytes.toString(CellUtil.cloneRow(results.get(0)))).getValue();
                    double lowerBound = getDistanceLowerBound(query, stringMeanRound, zeroStr);
                    for (Pair<Integer, Integer> position : indexNode.getPositions()) {
                        positions.add(new Interval(position.getFirst() + regionBegin, position.getSecond() + regionBegin, lowerBound));
                    }

                    indexCaches.get(index).addCache(stringMeanRound, indexNode);
                }
            }
        }
    }

    private void scanCache(int index, String begin, boolean beginInclusive, String end, boolean endInclusive, QuerySegment query, List<Interval> positions, String zeroStr) {
        for (Map.Entry entry : indexCaches.get(index).getCaches().subMap(begin, beginInclusive, end, endInclusive).entrySet()) {
            String stringMeanRound = (String) entry.getKey();
            IndexNode indexNode = (IndexNode) entry.getValue();
            double lowerBound = getDistanceLowerBound(query, stringMeanRound, zeroStr);
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

    @Override
    public void scanData(RpcController controller, BridgeQueryProtos.ScanDataRequest request, RpcCallback<BridgeQueryProtos.ScanDataResponse> done) {
        try {
            // get basic region information
            long regionBegin, regionEnd;
            regionBegin = 0;  // scan data between 1970.01.01 and now
            regionEnd = System.currentTimeMillis() / TimeSeriesNode.TIME_STEP;

            BridgeQueryProtos.ScanDataResponse.Builder responseBuilder = BridgeQueryProtos.ScanDataResponse.newBuilder();

            // judge whether position is in the region range, within - calculate, intersection - send back directly, no - ignore
            int cntCandidate = 0;
            for (int index = 0; index < request.getPositionsBeginCount(); index++) {
                cntCandidate += request.getPositionsEnd(index) - request.getPositionsBegin(index) + 1;

                long left = request.getPositionsBegin(index);
                long right = request.getPositionsEnd(index) + request.getM() - 1;

                long begin = left / request.getWr() * request.getWr();
                long end = right / request.getWr() * request.getWr();

                if (end < regionBegin || begin >= regionEnd) {
                    // ignore, do noting in this region
                } else if (begin >= regionBegin && end < regionEnd) {
                    // within
                    logger.debug("Scan data [{}, {}]", left, right);
                    List<Float> data = getTimeSeries(request.getChannelCode(), left, right - left + 1, begin, end, request.getWr());

                    for (int i = 0; i + request.getM() - 1 < data.size(); i++) {
                        double distance = 0;
                        for (int j = 0; j < request.getM() && distance <= request.getEpsilon() * request.getEpsilon(); j++) {
                            distance += (data.get(i + j) - request.getQuery(j)) * (data.get(i + j) - request.getQuery(j));
                        }
                        if (distance <= request.getEpsilon() * request.getEpsilon()) {
                            responseBuilder.addAnswersPosition(left + i);
                            responseBuilder.addAnswersDistance(Math.sqrt(distance));
                        }
                    }
                } else if (begin < regionEnd && regionEnd <= end) {  // avoid duplicate with other regions
                    // intersection, leave for client to scan data
                    logger.debug("Leave for client [{}, {}]", left, right);
                    responseBuilder.addPositionsBegin(request.getPositionsBegin(index));
                    responseBuilder.addPositionsEnd(request.getPositionsEnd(index));
                }
            }

            BridgeQueryProtos.ScanDataResponse response = responseBuilder.setCntCandidate(cntCandidate).build();
            done.run(response);
        } catch (IOException ioe) {
            logger.error(ioe.getMessage(), ioe.getCause());
            ResponseConverter.setControllerException(controller, ioe);
        }
    }

    private List<Float> getTimeSeries(String channelCode, long left, long length, long begin, long end, int Wr) throws IOException {
        List<Float> ret = new ArrayList<>();

        if (begin == end) {  // `get` is faster
            TimeSeriesNode node = dataCaches.get(begin);  // try to use dataCaches first
            if (node == null) {
                Get get = new Get(Bytes.toBytes(new TimeSeriesRowKey(channelCode, (begin - 1) * TimeSeriesNode.TIME_STEP).toString()));
                get.addColumn(Bytes.toBytes("value"), Bytes.toBytes("data"));
                Result result = env.getRegion().get(get);

                node = new TimeSeriesNode();
                node.parseBytes(begin * TimeSeriesNode.TIME_STEP, result.getValue(Bytes.toBytes("value"), Bytes.toBytes("data")));

                dataCaches.clear();
                dataCaches.put(begin, node);
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

            try (InternalScanner scanner = env.getRegion().getScanner(scan)) {
                boolean finished = false;
                Pair<Long, Float> lastNode = new Pair<>();
                long lastOffset = 0;
                while (!finished) {
                    List<Cell> results = new ArrayList<>();
                    finished = !scanner.next(results);

                    if (!results.isEmpty()) {
                        TimeSeriesNode node = new TimeSeriesNode();
                        TimeSeriesRowKey tmpRowKey = new TimeSeriesRowKey(CellUtil.cloneRow(results.get(0)));
                        node.parseBytes(tmpRowKey.getFirst(), CellUtil.cloneValue(results.get(0)));

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
                        lastOffset = tmpRowKey.getFirst();
                        lastNode = node.getLastNode();
                    }

                }
            }
        }
        if (ret.size() > length)

        {
            throw new IOException("data fetch mistake!");
        }
        return ret;
    }

    private double getDistanceLowerBound(QuerySegment query, String stringMeanRound, String zeroStr) {
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

    private List<QuerySegment> optimizeQueryOrder(BridgeQueryProtos.QueryRequest request, String prefix) throws IOException {
        List<QuerySegment> queries = new ArrayList<>(request.getMeansCount());

        // get statistic information
        byte[] statisticData = CellUtil.cloneValue(env.getRegion().get(new Get(Bytes.toBytes(prefix + "statistic")), false).get(0));
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
        double range = request.getEpsilon() / Math.sqrt(request.getWu());
        for (int i = 0; i < request.getMeansCount(); i++) {
            double meanQ = request.getMeans(i);

            String begin = String.valueOf(Double.doubleToLongBits(meanQ - range));
            String beginRound = (begin.charAt(0) == '-' ? begin.substring(0, request.getUr() + 1) : begin.substring(0, request.getUr()));
            String end = String.valueOf(Double.doubleToLongBits(meanQ + range));
            String endRound = ((end.charAt(0) == '-' ? end.substring(0, request.getUr() + 1) : end.substring(0, request.getUr())) + "1");

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

    @Override
    public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        if (coprocessorEnvironment instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) coprocessorEnvironment;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        dataCaches.clear();
        indexCaches.clear();
    }

    @Override
    public Service getService() {
        return this;
    }

    private static class IndexBuilder {

        long first = -1;      // left offset, for output global position of time series
        InternalScanner scanner;
        RegionCoprocessorEnvironment env;
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

        IndexBuilder(String channelCode, int Wr, int Wu, int Ur, long regionBegin, long regionEnd, InternalScanner scanner, RegionCoprocessorEnvironment env) throws IOException {
            this.scanner = scanner;
            this.Wr = Wr;
            this.Wu = Wu;
            this.channelCode = channelCode;
            this.Ur = Ur;
            this.regionBegin = regionBegin / TimeSeriesNode.TIME_STEP;
            this.regionBegin = getStatisticSplitPoint(0);
            this.regionEnd = regionEnd / TimeSeriesNode.TIME_STEP;
            this.env = env;
            this.table = env.getTable(TableName.valueOf("indextable"));

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
                    List<Cell> results = new ArrayList<>();
                    done = !scanner.next(results);
                    long tmpOffset = new TimeSeriesRowKey(Bytes.toString(CellUtil.cloneRow(results.get(0)))).getFirst();
                    node = new TimeSeriesNode();
                    node.parseBytes(tmpOffset, CellUtil.cloneValue(results.get(0)));

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
            long splitPoint = getStatisticSplitPoint(1);

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
                            long loc = (it) * (EPOCH - Wu + 1) + i - Wu + 1 + 1 + first - 1 + missNum;
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
        }

    }
}
