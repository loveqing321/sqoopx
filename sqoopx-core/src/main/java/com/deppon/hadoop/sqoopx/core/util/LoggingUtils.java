package com.deppon.hadoop.sqoopx.core.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Created by meepai on 2017/6/19.
 */
public class LoggingUtils {

    /**
     * 设置调试级别
     */
    public static void setDebugLevel(){
        Logger.getLogger("com.deppon.hadoop.sqoopx").setLevel(Level.DEBUG);
    }
}
