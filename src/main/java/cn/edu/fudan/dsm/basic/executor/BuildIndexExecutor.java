package cn.edu.fudan.dsm.basic.executor;

import cn.edu.fudan.dsm.basic.common.entity.IndexNode;
import cn.edu.fudan.dsm.basic.common.entity.IndexRowKey;
import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesNode;
import cn.edu.fudan.dsm.basic.common.entity.TimeSeriesRowKey;
import cn.edu.fudan.dsm.basic.coprocessor.generated.BridgeQueryProtos;
import cn.edu.fudan.dsm.basic.operator.IndexOperator;
import cn.edu.fudan.dsm.basic.operator.TimeSeriesOperator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.WriteAbortedException;
import java.util.*;

/**
 * Created by huibo on 2016/12/7.
 */
public class BuildIndexExecutor implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(BuildIndexExecutor.class.getName());

    private String channelCode;
    private int Wr, Wu, Ur;

    private TimeSeriesOperator timeSeriesOperator;
    private IndexOperator indexOperator;

    public BuildIndexExecutor(String channelCode, int Wr, int Wu, int Ur, boolean rebuild) throws IOException {
        this.channelCode = channelCode;
        this.Wr = Wr;
        this.Wu = Wu;
        this.Ur = Ur;

        timeSeriesOperator = new TimeSeriesOperator(channelCode, Wr, false);
        String firstRowKey = timeSeriesOperator.getFirstRowKey(false);
        String lastRowKey = timeSeriesOperator.getFirstRowKey(true);
        indexOperator = new IndexOperator(new TimeSeriesRowKey(firstRowKey).getFirst(),
                new TimeSeriesRowKey(lastRowKey).getFirst(), rebuild, !rebuild);
    }

    public void execute() {
        long startTime = System.currentTimeMillis();

        try {
            final BridgeQueryProtos.BuildIndexRequest request = BridgeQueryProtos.BuildIndexRequest.newBuilder().setChannelCode(channelCode).setWr(Wr).setWu(Wu).setUr(Ur).build();
            Map<byte[], Boolean> results = timeSeriesOperator.getTable().coprocessorService(BridgeQueryProtos.BridgeQueryService.class, null, null,
                    new Batch.Call<BridgeQueryProtos.BridgeQueryService, Boolean>() {
                        public Boolean call(BridgeQueryProtos.BridgeQueryService result) throws IOException {
                            BlockingRpcCallback<BridgeQueryProtos.BuildIndexResponse> rpcCallback = new BlockingRpcCallback<>();
                            result.buildIndex(null, request, rpcCallback);
                            BridgeQueryProtos.BuildIndexResponse response = rpcCallback.get();
                            return response.getSuccess();
                        }
                    }
            );

            for (Map.Entry<byte[], Boolean> entry : results.entrySet()) {
                logger.info("Region: {} - Success?: {}", Bytes.toString(entry.getKey()), entry.getValue());
            }
            logger.info("Coprocessor time usage: {} ms", System.currentTimeMillis() - startTime);

            // process region borders
            long startTime1 = System.currentTimeMillis();

            byte[][] startKeys = indexOperator.getRegionLocator().getStartKeys();
            for (int i = 1; i < startKeys.length; i++) {
                long regionStart = Long.parseLong(Bytes.toString(startKeys[i]));
                IndexNode tmpNode = indexOperator.getIndexNode(regionStart + "_" + channelCode + "_statistic");
                if (tmpNode.getPositions().size() == 0) {
                    long regionBegin = regionStart - Wr * TimeSeriesNode.TIME_STEP;
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTimeInMillis(regionStart);
                    calendar.add(Calendar.MONTH, 1);
                    long regionEnd = calendar.getTimeInMillis() + Wr * TimeSeriesNode.TIME_STEP;
                    Scan scan = new Scan(Bytes.toBytes(channelCode + "_" + regionBegin), Bytes.toBytes(channelCode + "_" + regionEnd)).setCaching(500);
                    ResultScanner resultScanner = timeSeriesOperator.getTable().getScanner(scan);
                    IndexBuilder indexBuilder = new IndexBuilder(channelCode, Wu, Ur, regionBegin, regionEnd, resultScanner, indexOperator);
                    indexBuilder.run();
                }

//                long nextRowBegin = (regionSplit + Wr - 2) / Wr * Wr + 1;
//                data = timeSeriesOperator.getTimeSeries(regionSplit, (int) (nextRowBegin - regionSplit + Wu - 1 + 1));  // TODO  // [s, t+Wu-2]
//                if (data.size() >= Wu) {
//                    IndexBuilder indexBuilder = new IndexBuilder(channelCode, Wu, Ur, -1, regionSplit, data, indexOperator);
//                    indexBuilder.run();
//                }
            }
            logger.info("Border process time usage: {} ms", System.currentTimeMillis() - startTime1);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        logger.info("Total time usage: {} ms", endTime - startTime);
//        StatisticWriter.println(String.valueOf(endTime - startTime));
    }

    private static class IndexBuilder {

        String channelCode;
        long first = -1;      // left offset, for output global position of time series
        ResultScanner scanner;
        IndexOperator indexOperator;
        int Ur;

        double[] t;  // data array and query array

        double d;
        double ex, ex2, mean, std;
        int Wu = -1;
        double[] buffer;
        boolean done = false;

        // For every EPOCH points, all cumulative values, such as ex (sum), ex2 (sum square), will be restarted for reducing the floating point error.
        int EPOCH = 100000;

        int dataIndex = 0;
        int missNum = 0;
        long regionEnd, regionBegin;
        TimeSeriesNode node = new TimeSeriesNode();
        List<Float> nodeData = new ArrayList<>();
        Pair<Long, Float> lastNode = null;
        long tmpMissNum = 0;
        long lastOffset = 0;

        IndexBuilder(String channelCode, int Wu, int Ur, long regionBegin, long regionEnd, ResultScanner scanner, IndexOperator indexOperator) throws IOException {
            this.scanner = scanner;
            this.Wu = Wu;
            this.channelCode = channelCode;
            this.Ur = Ur;
            this.regionBegin = regionBegin / TimeSeriesNode.TIME_STEP;
            this.regionBegin = getStatisticSplitPoint(0);
            this.regionEnd = regionEnd / TimeSeriesNode.TIME_STEP;
            this.indexOperator = indexOperator;

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
            List<Put> puts = new ArrayList<>(indexNodeMap.size());
            List<Pair<String, Integer>> statisticInfo = new ArrayList<>(indexNodeMap.size());
            long startT = startTimestamp * TimeSeriesNode.TIME_STEP;
            String prefix = startT + "_" + channelCode + "_"; // the rowKey of index table likes timestampOfMonth_channelCode_mean
            for (Map.Entry entry : indexNodeMap.entrySet()) {
                Put put = new Put(Bytes.toBytes(prefix + entry.getKey()));
                IndexNode indexNodeCurrent = (IndexNode) entry.getValue();
                IndexNode indexNodeOrigin = indexOperator.getIndexNode(prefix + entry.getKey());
                if (indexNodeOrigin != null) {
                    indexNodeOrigin.getPositions().addAll(indexNodeCurrent.getPositions());
                    indexNodeOrigin.setPositions(sortAndMergeIntervals(indexNodeOrigin.getPositions()));
                    put.addColumn(Bytes.toBytes("std"), Bytes.toBytes("p"), indexNodeOrigin.toBytes());
                    statisticInfo.add(new Pair<>((String) entry.getKey(), indexNodeOrigin.getPositions().size()));
                } else {
                    put.addColumn(Bytes.toBytes("std"), Bytes.toBytes("p"), indexNodeCurrent.toBytes());
                    statisticInfo.add(new Pair<>((String) entry.getKey(), indexNodeCurrent.getPositions().size()));
                }
                puts.add(put);
            }
            indexOperator.putList(puts);
//            logger.info("Put the data:{}, number:{} into HBase.", puts.toString(), put.size());

            IndexNode tmp = indexOperator.getIndexNode(prefix + "statistic");
            if (tmp.getPositions().size() != 0) return;

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
            indexOperator.put(put);
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

    @Override
    public void close() throws IOException {

    }
}
