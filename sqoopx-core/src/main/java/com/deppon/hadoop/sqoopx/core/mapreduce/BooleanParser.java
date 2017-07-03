package com.deppon.hadoop.sqoopx.core.mapreduce;

/**
 * Created by meepai on 2017/7/3.
 */
public final class BooleanParser {

    public static boolean valueOf(String s){
        return s != null && ("true".equalsIgnoreCase(s) || "t".equalsIgnoreCase(s)
                || "1".equalsIgnoreCase(s) || "on".equalsIgnoreCase(s) || "yes".equalsIgnoreCase(s));
    }
}
