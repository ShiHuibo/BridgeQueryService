package cn.edu.fudan.dsm.basic.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by huibo on 2016/12/21.
 */
public class QueryTest {

    private static final Logger logger = LoggerFactory.getLogger(QueryTest.class);

    public static void main(String args[]) {
        query(args[0], Long.parseLong(args[1]));
    }

    private static void query(String channelCode, long offset) {
//        StatisticWriter.println("Query");
        int Wr = 60, Wu = 50, Ur = 5, M = 1280, Epsilon = 10;

//        StatisticWriter.println("N,R,Wr,Wu,Ur,Er,M,Epsilon,Time (ms) Average,Minimum,Maximum");
        QueryExecutor queryExecutor;

        try {
            queryExecutor = new QueryExecutor(channelCode, Wr, Wu, Ur, true, M, Epsilon);
            queryExecutor.execute(offset);
            queryExecutor.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }
}
