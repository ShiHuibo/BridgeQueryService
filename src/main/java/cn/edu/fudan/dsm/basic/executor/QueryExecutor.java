package cn.edu.fudan.dsm.basic.executor;

import cn.edu.fudan.dsm.basic.coprocessor.generated.BridgeQueryProtos;
import cn.edu.fudan.dsm.basic.operator.IndexOperator;
import cn.edu.fudan.dsm.basic.operator.TimeSeriesOperator;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Created by huibo on 2016/12/10.
 */
public class QueryExecutor implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class.getName());

    public static long COPROCESSOR_INDEX_TIME_USAGE;

    private String channelCode;
    private int Wr, Wu, Ur, M;
    private double Epsilon;
    private boolean useCache;

    private TimeSeriesOperator timeSeriesOperator;
    private IndexOperator indexOperator;

    public QueryExecutor(String channelCode, int Wr, int Wu, int Ur, boolean useCache, int M, double Epsilon) throws IOException {
        this.channelCode = channelCode;
//        this.R = R;  // # of regions
        this.Wr = Wr;  // data row length 1024 -> 60
        this.Wu = Wu;  // window length
        this.Ur = Ur;  //  6
//        double a = 1.1;
//        String rowkey = String.valueOf(Double.doubleToLongBits(a)).substring(Ur);
        this.useCache = useCache; //yes
        this.M = M;  // query length
        this.Epsilon = Epsilon;

        timeSeriesOperator = new TimeSeriesOperator(channelCode, Wr, false);
        indexOperator = new IndexOperator(0, 0, false, false);
    }

    public List<Float> getAnswer(long offset) {
        return timeSeriesOperator.getTimeSeries(offset, M);
    }

    public Pair<Integer, List<Pair<Long, Double>>> queryOnce(List<Float> query, long offset) {
        query.addAll(timeSeriesOperator.getTimeSeries(offset, M));
        return queryOnce(query);
    }

    public Pair<Integer, List<Pair<Long, Double>>> queryOnce(List<Float> query) {
        // calculate mean and std of each disjoint window
        List<Double> means = new ArrayList<>(M / Wu);
        double ex = 0;

        for (int i = 0; i < M && i < query.size(); i++) {
            double data = query.get(i);

            // Calculate sum and sum square
            ex += data;

            if ((i + 1) % Wu == 0) {
                double mean = ex / Wu;
                means.add(mean);

                ex = 0;
            }
        }

        if (query.size() < Wu) {
            throw new IllegalArgumentException("Query series is too short! minimum = " + Wu);
        }

        List<Pair<Long, Double>> answers = new ArrayList<>();
        List<Pair<Long, Long>> validPositions = new ArrayList<>();
        final int[] cntFinalCandidate = {0};

        COPROCESSOR_INDEX_TIME_USAGE = System.currentTimeMillis();

        try {
            final BridgeQueryProtos.QueryRequest request = BridgeQueryProtos.QueryRequest.newBuilder().setChannelCode(channelCode).setWr(Wr).setWu(Wu).setUr(Ur).setM(M).setEpsilon(Epsilon).addAllMeans(means).setUseCache(useCache).build();
            Map<byte[], Pair<List<Long>, List<Long>>> results = indexOperator.getTable().coprocessorService(BridgeQueryProtos.BridgeQueryService.class, null, null,
                    new Batch.Call<BridgeQueryProtos.BridgeQueryService, Pair<List<Long>, List<Long>>>() {
                        public Pair<List<Long>, List<Long>> call(BridgeQueryProtos.BridgeQueryService result) throws IOException {
                            BlockingRpcCallback<BridgeQueryProtos.QueryResponse> rpcCallback = new BlockingRpcCallback<>();
                            result.query(null, request, rpcCallback);
                            BridgeQueryProtos.QueryResponse response = rpcCallback.get();
                            return new Pair<>(response.getPositionsBeginList(), response.getPositionsEndList());
                        }
                    }
            );

            COPROCESSOR_INDEX_TIME_USAGE = System.currentTimeMillis() - COPROCESSOR_INDEX_TIME_USAGE;
            logger.info("Coprocessor index time usage: {} ms", COPROCESSOR_INDEX_TIME_USAGE);

            // use coprocessor to scan data
            BridgeQueryProtos.ScanDataRequest.Builder request2Builder = BridgeQueryProtos.ScanDataRequest.newBuilder().setChannelCode(channelCode).setWr(Wr).setWu(Wu).setUr(Ur).setM(M).setEpsilon(Epsilon).addAllQuery(query);
            for (Map.Entry<byte[], Pair<List<Long>, List<Long>>> entry : results.entrySet()) {
                request2Builder.addAllPositionsBegin(entry.getValue().getFirst());
                request2Builder.addAllPositionsEnd(entry.getValue().getSecond());
            }
            final BridgeQueryProtos.ScanDataRequest request2 = request2Builder.build();

            Map<byte[], Pair<List<Pair<Long, Double>>, List<Pair<Long, Long>>>> results2 = timeSeriesOperator.getTable().coprocessorService(BridgeQueryProtos.BridgeQueryService.class, null, null,
                    new Batch.Call<BridgeQueryProtos.BridgeQueryService, Pair<List<Pair<Long, Double>>, List<Pair<Long, Long>>>>() {
                        public Pair<List<Pair<Long, Double>>, List<Pair<Long, Long>>> call(BridgeQueryProtos.BridgeQueryService result) throws IOException {
                            BlockingRpcCallback<BridgeQueryProtos.ScanDataResponse> rpcCallback = new BlockingRpcCallback<>();
                            result.scanData(null, request2, rpcCallback);
                            BridgeQueryProtos.ScanDataResponse response = rpcCallback.get();

                            List<Pair<Long, Double>> answers = new ArrayList<>(response.getAnswersPositionCount());
                            for (int i = 0; i < response.getAnswersPositionCount(); i++) {
                                answers.add(new Pair<>(response.getAnswersPosition(i), response.getAnswersDistance(i)));
                            }

                            cntFinalCandidate[0] += response.getCntCandidate();

                            List<Pair<Long, Long>> positions = new ArrayList<>(response.getPositionsBeginCount());
                            for (int i = 0; i < response.getPositionsBeginCount(); i++) {
                                positions.add(new Pair<>(response.getPositionsBegin(i), response.getPositionsEnd(i)));
                            }
                            return new Pair<>(answers, positions);
                        }
                    }
            );
            for (Map.Entry<byte[], Pair<List<Pair<Long, Double>>, List<Pair<Long, Long>>>> entry : results2.entrySet()) {
                answers.addAll(entry.getValue().getFirst());
                validPositions.addAll(entry.getValue().getSecond());
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        int cntCandidates = cntFinalCandidate[0];
        for (Pair<Long, Long> position : validPositions) {
            cntCandidates += position.getSecond() - position.getFirst() + 1;

            long begin = position.getFirst();
            long end = position.getSecond() + M - 1;
            logger.debug("Scan data [{}, {}]", begin, end);
            List<Float> data = timeSeriesOperator.getTimeSeries(begin, end - begin + 1);

            for (int i = 0; i + M - 1 < data.size(); i++) {
                double distance = 0;
                for (int j = 0; j < M && distance <= Epsilon * Epsilon; j++) {
                    distance += (data.get(i + j) - query.get(j)) * (data.get(i + j) - query.get(j));
                }
                if (distance <= Epsilon * Epsilon) {
                    answers.add(new Pair<>(begin + i, Math.sqrt(distance)));
                }
            }
        }

        Collections.sort(answers, new Comparator<Pair<Long, Double>>() {
            @Override
            public int compare(Pair<Long, Double> o1, Pair<Long, Double> o2) {
                return o1.getSecond().compareTo(o2.getSecond());
            }
        });

        return new Pair<>(cntCandidates, answers);
    }

    //don't read
    public void execute(long offset) {
        long startTime = System.currentTimeMillis();

//        for (int i = 0; i < 100; i++) {  // query 100 times to get average info
//            queryByCoprocessor(statisticInfo, statisticInfo2, statisticInfo3, statisticInfo4, statisticInfo5, queryOffsets.get(i));
        queryByCoprocessor(offset);
//        }

        long endTime = System.currentTimeMillis();

        logger.info("Time usage in total: {} ms", endTime - startTime);
    }

    private void queryByCoprocessor(long left) {
        // fetch a substring of original time series, randomly pick left position
//        long left = ThreadLocalRandom.current().nextLong(1, N - M + 1);
        logger.info("Query offset: {}", left);
        List<Float> query = timeSeriesOperator.getTimeSeries(left, M);

        long startTime = System.currentTimeMillis();

        // calculate mean and std of each disjoint window
        List<Double> means = new ArrayList<>(M / Wu);
        double ex = 0;

        for (int i = 0; i < M && i < query.size(); i++) {
            double data = query.get(i);

            // Calculate sum and sum square
            ex += data;

            if ((i + 1) % Wu == 0) {
                double mean = ex / Wu;
                means.add(mean);

                ex = 0;
            }
        }

        List<Pair<Long, Double>> answers = new ArrayList<>();
        List<Pair<Long, Long>> validPositions = new ArrayList<>();

//        final int[] cntCandidates = new int[statisticInfo3.size()];
        final int[] cntFinalCandidate = {0};

        long startTime2 = 0;
        try {
//            DataQuery dataQuery = new DataQuery();
//            dataQuery.query(channelCode, Wu, Ur, Epsilon, means, useCache);
            final BridgeQueryProtos.QueryRequest request = BridgeQueryProtos.QueryRequest.newBuilder().setChannelCode(channelCode).setWr(Wr).setWu(Wu).setUr(Ur).setM(M).setEpsilon(Epsilon).addAllMeans(means).setUseCache(useCache).build();
            Map<byte[], Pair<List<Long>, List<Long>>> results = indexOperator.getTable().coprocessorService(BridgeQueryProtos.BridgeQueryService.class, null, null,
                    new Batch.Call<BridgeQueryProtos.BridgeQueryService, Pair<List<Long>, List<Long>>>() {
                        public Pair<List<Long>, List<Long>> call(BridgeQueryProtos.BridgeQueryService result) throws IOException {
                            BlockingRpcCallback<BridgeQueryProtos.QueryResponse> rpcCallback = new BlockingRpcCallback<>();
                            result.query(null, request, rpcCallback);
                            BridgeQueryProtos.QueryResponse response = rpcCallback.get();
//                            for (int i = 0; i < response.getCntCandidatesCount()-1; i++) {  // for candidate statistic
//                                cntCandidates[i] += response.getCntCandidates(i);
//                            }
//                            cntCandidates[cntCandidates.length - 1] += response.getCntCandidates(response.getCntCandidatesCount() - 1);  // cnt of rows scanned
                            return new Pair<>(response.getPositionsBeginList(), response.getPositionsEndList());
                        }
                    }
            );

            long endTime = System.currentTimeMillis();
//            logger.info("Coprocessor index time usage: {} ms, {} rows scanned.", endTime - startTime, cntCandidates[cntCandidates.length - 1]);

            startTime2 = System.currentTimeMillis();

            // use coprocessor to scan data
//            cntFinalCandidate[0] += dataQuery.scanData(channelCode, Wr, M, Epsilon, query);
//            answers.addAll(dataQuery.answers);
//            validPositions.addAll(dataQuery.scandataPositions);
            BridgeQueryProtos.ScanDataRequest.Builder request2Builder = BridgeQueryProtos.ScanDataRequest.newBuilder().setChannelCode(channelCode).setWr(Wr).setWu(Wu).setUr(Ur).setM(M).setEpsilon(Epsilon).addAllQuery(query);
            for (Map.Entry<byte[], Pair<List<Long>, List<Long>>> entry : results.entrySet()) {
                request2Builder.addAllPositionsBegin(entry.getValue().getFirst());
                request2Builder.addAllPositionsEnd(entry.getValue().getSecond());
            }
            final BridgeQueryProtos.ScanDataRequest request2 = request2Builder.build();

            Map<byte[], Pair<List<Pair<Long, Double>>, List<Pair<Long, Long>>>> results2 = timeSeriesOperator.getTable().coprocessorService(BridgeQueryProtos.BridgeQueryService.class, null, null,
                    new Batch.Call<BridgeQueryProtos.BridgeQueryService, Pair<List<Pair<Long, Double>>, List<Pair<Long, Long>>>>() {
                        public Pair<List<Pair<Long, Double>>, List<Pair<Long, Long>>> call(BridgeQueryProtos.BridgeQueryService result) throws IOException {
                            BlockingRpcCallback<BridgeQueryProtos.ScanDataResponse> rpcCallback = new BlockingRpcCallback<>();
                            result.scanData(null, request2, rpcCallback);
                            BridgeQueryProtos.ScanDataResponse response = rpcCallback.get();

                            List<Pair<Long, Double>> answers = new ArrayList<>(response.getAnswersPositionCount());
                            for (int i = 0; i < response.getAnswersPositionCount(); i++) {
                                answers.add(new Pair<>(response.getAnswersPosition(i), response.getAnswersDistance(i)));
                            }

                            cntFinalCandidate[0] += response.getCntCandidate();

                            List<Pair<Long, Long>> positions = new ArrayList<>(response.getPositionsBeginCount());
                            for (int i = 0; i < response.getPositionsBeginCount(); i++) {
                                positions.add(new Pair<>(response.getPositionsBegin(i), response.getPositionsEnd(i)));
                            }
                            return new Pair<>(answers, positions);
                        }
                    }
            );
            for (Map.Entry<byte[], Pair<List<Pair<Long, Double>>, List<Pair<Long, Long>>>> entry : results2.entrySet()) {
                answers.addAll(entry.getValue().getFirst());
                validPositions.addAll(entry.getValue().getSecond());
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        int cntCandidate = cntFinalCandidate[0];
        for (Pair<Long, Long> position : validPositions) {
            cntCandidate += position.getSecond() - position.getFirst() + 1;

            long begin = position.getFirst();
            long end = position.getSecond() + M - 1;
            logger.info("Scan data [{}, {}]", begin, end);
            List<Float> data = timeSeriesOperator.getTimeSeries(begin, end - begin + 1);

            for (int i = 0; i + M - 1 < data.size(); i++) {
                double distance = 0;
                for (int j = 0; j < M && distance <= Epsilon * Epsilon; j++) {
                    distance += (data.get(i + j) - query.get(j)) * (data.get(i + j) - query.get(j));
                }
                if (distance <= Epsilon * Epsilon) {
                    answers.add(new Pair<>(begin + i, Math.sqrt(distance)));
                }
            }
        }

        Collections.sort(answers, new Comparator<Pair<Long, Double>>() {
            @Override
            public int compare(Pair<Long, Double> o1, Pair<Long, Double> o2) {
                return o1.getSecond().compareTo(o2.getSecond());
            }
        });

        if (!answers.isEmpty() && answers.get(0).getFirst() == left && answers.get(0).getSecond() == 0) {
            logger.info("Best: {}, distance: {}", answers.get(0).getFirst(), answers.get(0).getSecond());
        } else {
            logger.warn("No sub-sequence within distance {}.", Epsilon);
        }

        long endTime2 = System.currentTimeMillis();
        logger.info("Total time usage: {} ms", endTime2 - startTime);
    }

    @Override
    public void close() throws IOException {
        timeSeriesOperator.close();
        indexOperator.close();
    }

}
