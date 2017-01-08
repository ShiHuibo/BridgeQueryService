package cn.edu.fudan.dsm.basic.common.entity;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by huibo on 2016/12/7.
 * <p>
 * a node of the index
 * </p>
 * Properties: positions(left, right)
 * Note: current timestamp = (left or right) * TIME_STEP + rowKey's timestamp
 */
public class IndexNode {

    private static int MAX_DIFF = 256;

    private List<Pair<Integer, Integer>> positions;

    public IndexNode() {
        this.positions = new ArrayList<>();
    }

    public byte[] toBytes() {
        /*
         * {left 1}{right 1}{left 2}{right 2}……{left n}{right n}
         */
        byte[] result = new byte[Bytes.SIZEOF_INT * positions.size() * 2];
        for (int i = 0; i < positions.size(); i++) {
            System.arraycopy(Bytes.toBytes(positions.get(i).getFirst()), 0, result, 2 * i * Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
            System.arraycopy(Bytes.toBytes(positions.get(i).getSecond()), 0, result, (2 * i + 1) * Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
        }
        return result;
    }

    public void parseBytes(byte[] concatData) {
        if (concatData == null) return;
        byte[] tmp = new byte[Bytes.SIZEOF_INT];
        positions.clear();
        for (int i = 0; i < concatData.length; i += 2 * Bytes.SIZEOF_INT) {
            System.arraycopy(concatData, i, tmp, 0, Bytes.SIZEOF_INT);
            int left = Bytes.toInt(tmp);
            System.arraycopy(concatData, i + Bytes.SIZEOF_INT, tmp, 0, Bytes.SIZEOF_INT);
            int right = Bytes.toInt(tmp);
            positions.add(new Pair<>(left, right));
        }
    }

    @Override
    public String toString() {
        return "IndexNode{position=" + positions + '}';
    }

    public List<Pair<Integer, Integer>> getPositions() {
        return positions;
    }

    public void setPositions(List<Pair<Integer, Integer>> positions) {
        this.positions = positions;
    }
}
