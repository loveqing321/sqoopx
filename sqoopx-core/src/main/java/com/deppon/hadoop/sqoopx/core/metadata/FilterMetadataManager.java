package com.deppon.hadoop.sqoopx.core.metadata;

import java.util.Map;

/**
 * 使用装饰者模式实现缓存功能的元数据管理器
 * Created by meepai on 2017/7/4.
 */
public class FilterMetadataManager implements MetadataManager {

    private MetadataManager delegate;

    private Map<String, Integer> columnTypes;

    private String[] columnNames;

    private String[] primaryKeys;

    public FilterMetadataManager(MetadataManager delegate){
        this.delegate = delegate;
    }

    public Map<String, Integer> getColumnTypes() {
        if(columnTypes == null){
            columnTypes = delegate.getColumnTypes();
        }
        return columnTypes;
    }

    public String[] getColumnNames() {
        if(columnNames == null){
            columnNames = delegate.getColumnNames();
        }
        return columnNames;
    }

    public String[] getPrimaryKeys() {
        if(primaryKeys == null){
            primaryKeys = delegate.getPrimaryKeys();
        }
        return primaryKeys;
    }
}
