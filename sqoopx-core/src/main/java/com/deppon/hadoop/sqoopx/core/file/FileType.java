package com.deppon.hadoop.sqoopx.core.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * 文件类型枚举
 * Created by meepai on 2017/6/19.
 */
public enum FileType {

    /**
     * Sequence文件
     */
    SEQUENCE_FILE,

    /**
     * Avro文件
     */
    AVRO_FILE,

    /**
     * Hcatalog managed文件
     */
    HCATALOG_MANAGED_FILE,

    /**
     * Parquet文件
     */
    PARQUET_FILE,

    /**
     * 未知文件类型
     */
    UNKNOWN;


    public static FileType getFileType(Path path, Configuration conf){
        return null;
    }
}
