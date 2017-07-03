package com.deppon.hadoop.sqoopx.core.util;

/**
 * Created by meepai on 2017/6/24.
 */
public class UriUtils {

    /**
     * 抽取URI中的schema部分
     * @param uri
     * @return
     */
    public static String extractSchema(String uri){
        int schemaStopIndex = uri.indexOf("//");
        if(schemaStopIndex == -1){
            schemaStopIndex = uri.indexOf(":");
            if(schemaStopIndex == -1){
                schemaStopIndex = uri.length();
            }
        }
        return uri.substring(0, schemaStopIndex);
    }
}
