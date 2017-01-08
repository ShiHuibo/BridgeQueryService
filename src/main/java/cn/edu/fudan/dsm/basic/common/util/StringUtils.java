package cn.edu.fudan.dsm.basic.common.util;

/**
 * Created by huibo on 2016/12/8.
 */
public class StringUtils {

    public static String toStringFixedWidth(long x, int width) {
        String str = String.valueOf(x);
        if (str.length() > width) {
            throw new IllegalArgumentException("width is too short (x: " + str + ", width: " + width + ")");
        }
        StringBuilder sb = new StringBuilder(str);
        for (int i = str.length(); i < width; i++) {
            sb.insert(0, "0");
        }
        return sb.toString();
    }

}
