package cn.edu.fudan.dsm.basic.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Indexer {

    private static final Logger logger = LoggerFactory.getLogger(Indexer.class.getName());

    // For cluster test
//    private static int minP = 7, maxP = 10;   // 10^7, 10^8, 10^9, 10^10
//    private static int minR = 1, maxR = 1;    // 7
//    private static int minWr = 2, maxWr = 2;  // 1024
//    private static int minWu = 2, maxWu = 2;  // 25, 50
//    private static int minUr = 1, maxUr = 1;  // 6
//    private static int minUc = 1, maxUc = 1;  // false, true
//    private static int minM = 7, maxM = 13;   // 128, 256, 512, 1024, 2048, 4096, 8192
//    private static int minEpsilon = 1, maxEpsilon = 2;  // 10, 100

    public static void main(String args[]) throws ClassNotFoundException, InterruptedException {

        // Build index (channelCode, rebuild or not)
        buildIndex(args[0], Boolean.parseBoolean(args[1]));

        // Query (different length, delta)
//        query(args[0]);

//        deleteTables();
    }

    private static void buildIndex(String channelCode, boolean rebuild) {
//        StatisticWriter.println("Build index");

//        StatisticWriter.println("N,R,Wr,Wu,Ur,Er,Time (ms),Storage (MB)");
        BuildIndexExecutor buildIndexExecutor;

        int Wr = 60, Wu = 50, Ur = 5;

        try {
            buildIndexExecutor = new BuildIndexExecutor(channelCode, Wr, Wu, Ur, rebuild);
//            buildIndexExecutor.splitRegion(new Date(2008,12,2).getTime());
            buildIndexExecutor.execute();
            buildIndexExecutor.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }

    }

}