package com.deppon.hadoop.sqoopx.core.metadata;

import java.util.Map;

/**
 * 外部源的元数据管理器
 * Created by meepai on 2017/7/4.
 */
public interface MetadataManager {

    /**
     * 获取列名与类型关联关系
     * @param
     * @return
     */
    Map<String, Integer> getColumnTypes();

    /**
     * 获取列名
     * @return
     */
    String[] getColumnNames();

    /**
     * 获取主键
     * @return if null return new String[0]
     */
    String[] getPrimaryKeys();
}
