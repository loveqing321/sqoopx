package com.deppon.hadoop.sqoopx.core.util;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import java.io.IOException;
import java.util.*;

/**
 * Created by meepai on 2017/7/4.
 */
public class HCatHelper {

    private static final String DEFHCATDB = "default";

    private Job job;

    private SqoopxOptions options;

    private String hCatDatabaseName;

    private String hCatTableName;

    private String hCatQualifiedTableName;

    private List<String> hCatPartitionKeys;

    private List<String> hCatPartitionValues;

    private Map<String, String> userHiveMapping;

    private HCatSchema hCatFullTableSchema;

    public HCatHelper(Job job, SqoopxOptions options){
        this.job = job;
        this.options = options;
        this.init();
    }

    private void init(){
        this.hCatDatabaseName = options.getHCatDatabaseName() != null ? options.getHCatDatabaseName() : DEFHCATDB;
        this.hCatTableName = options.getHCatTableName().toLowerCase();
        this.hCatQualifiedTableName = hCatDatabaseName + "." + hCatTableName;
        this.hCatPartitionKeys = new ArrayList<String>();
        this.hCatPartitionValues = new ArrayList<String>();
        // 优先使用hCatalog分区键值，其次使用hive分区键值。
        String partitionKeys = options.getHCatalogPartitionKeys();
        String partitionValues = options.getHCatalogPartitionValues();
        if(partitionKeys != null){
            String[] keys = partitionKeys.split(",");
            for(int i=0; i<keys.length; i++){
                this.hCatPartitionKeys.add(keys[i].trim());
            }
            String[] values = partitionValues.split(",");
            for(int i=0; i<values.length; i++){
                this.hCatPartitionValues.add(values[i].trim());
            }
        } else {
            partitionKeys = options.getHivePartitionKey();
            partitionValues = options.getHivePartitionValue();
            if(partitionKeys != null){
                this.hCatPartitionKeys.add(partitionKeys);
                this.hCatPartitionValues.add(partitionValues);
            }
        }
        this.userHiveMapping = new HashMap<String, String>();
        Properties props = options.getMapColumnHive();
        for(Object key : props.keySet()){
            this.userHiveMapping.put(((String)key).trim(), (String) props.get(key));
        }
    }

    /**
     * 配置HCat的inputFormat
     */
    public void configureHCatInputFormat() throws IOException {
        // 1. 获取
        String filter = getHCatFilter();
        HCatInputFormat.setInput(job, hCatDatabaseName, hCatTableName, filter);
        hCatFullTableSchema = HCatInputFormat.getTableSchema(job.getConfiguration());
        List<String> fieldNames = hCatFullTableSchema.getFieldNames();
    }

    /**
     * @throws IOException
     */
    public void configureHCatOutputFormat() throws IOException {
        Map<String, String> filterMap = getHCatFilterMap();
        HCatOutputFormat.setOutput(job, OutputJobInfo.create(hCatDatabaseName, hCatTableName, filterMap));
        HCatSchema hCatOutputSchema = HCatOutputFormat.getTableSchema(job.getConfiguration());
        HCatOutputFormat.setSchema(job, hCatOutputSchema);
    }

    /**
     * 创建HCatTable
     */
    public void createHCatTable(){

    }

    /**
     * 根据分区键值来构建过滤字符串
     * @return
     */
    private String getHCatFilter(){
        if(this.hCatPartitionKeys != null && this.hCatPartitionKeys.size() > 0){
            StringBuilder sb = new StringBuilder();
            for(int i=0; i<this.hCatPartitionKeys.size(); i++){
                String key = this.hCatPartitionKeys.get(i);
                String val = this.hCatPartitionValues.get(i);
                if(i > 0){
                    sb.append(" AND ");
                }
                sb.append(key).append("=").append("\"").append(val).append("\"");
            }
            return sb.toString();
        }
        return null;
    }

    /**
     * 根据分区键值来构建过滤字符串
     * @return
     */
    private Map<String, String> getHCatFilterMap(){
        if(this.hCatPartitionKeys != null && this.hCatPartitionKeys.size() > 0){
            Map<String, String> map = new HashMap<String, String>();
            for(int i=0; i<this.hCatPartitionKeys.size(); i++){
                String key = this.hCatPartitionKeys.get(i);
                String val = this.hCatPartitionValues.get(i);
                map.put(key, val);
            }
            return map;
        }
        return null;
    }
}
