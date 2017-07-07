package com.deppon.hadoop.sqoopx.core.tools.job;

import com.deppon.hadoop.sqoopx.core.codegen.DefaultClassWriter;
import com.deppon.hadoop.sqoopx.core.conf.ConfigurationConstants;
import com.deppon.hadoop.sqoopx.core.conf.ConfigurationHelper;
import com.deppon.hadoop.sqoopx.core.mapreduce.ExportRecordMapper;
import com.deppon.hadoop.sqoopx.core.mapreduce.HCatExportMapper;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.util.HCatHelper;
import com.deppon.hadoop.sqoopx.core.util.Jars;
import com.deppon.hadoop.sqoopx.core.util.SqlUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by meepai on 2017/6/25.
 */
public class HiveExportJob extends BaseJarJob {

    private HCatHelper hCatHelper;

    public HiveExportJob(){
        super("sqoopx-hive-export-");
    }

    @Override
    protected void doRun(SqoopxJobContext context) throws Exception {
        this.hCatHelper = new HCatHelper(context);
        // 配置并执行job
        this.configureJobAndRun(context.getOptions().getConf(), context.getOptions(), codeGenerator.getOutJar());
    }

    @Override
    protected void configureInputFormat(Job job, SqoopxOptions options) throws IOException {
        // 1. 设置InputFormat
        this.hCatHelper.configureHCatInputFormat(job);

        Map<String, Integer> types = this.metadataManager.getColumnTypes();
        MapWritable columnTypesJava = new MapWritable();
        MapWritable columnTypesSql = new MapWritable();
        Properties mapColumnJava = options.getMapColumnJava();
        for(Map.Entry<String, Integer> entry : types.entrySet()){
            Text columnName = new Text(entry.getKey());
            Text columnType = null;
            if(mapColumnJava.containsKey(entry.getKey())){
                columnType = new Text(mapColumnJava.getProperty(entry.getKey()));
            } else {
                columnType = new Text(SqlUtils.toJavaType(entry.getValue()));
            }
            columnTypesJava.put(columnName, columnType);
            columnTypesSql.put(columnName, new IntWritable(entry.getValue()));
        }
        // 序列化到配置项中，在mapreduce任务中获取并使用。
        DefaultStringifier.store(job.getConfiguration(), columnTypesJava, ConfigurationConstants.HCAT_OUTPUT_COLTYPES_JAVA);
        DefaultStringifier.store(job.getConfiguration(), columnTypesSql, ConfigurationConstants.HCAT_OUTPUT_COLTYPES_SQL);
    }

    @Override
    protected void configureMapper(Job job, SqoopxOptions options) throws IOException {
        // 设置mapper的任务数量
        ConfigurationHelper.setJobMapTasks(job, 2);
        // 配置mapper
        job.setMapperClass(HCatExportMapper.class);
        // 设置mapper的输出类型
        job.setMapOutputKeyClass(this.codeGenerator.getGenerateClass());
        job.setMapOutputValueClass(NullWritable.class);
    }

    @Override
    protected void configureOutputFormat(Job job, SqoopxOptions options) throws IOException {
        job.setOutputFormatClass(DBOutputFormat.class);
        if(options.getUsername() != null){
            DBConfiguration.configureDB(job.getConfiguration(), options.getDriverClassName(),
                    options.getConnectString(), options.getUsername(), options.getPassword());
        } else {
            DBConfiguration.configureDB(job.getConfiguration(), options.getDriverClassName(),
                    options.getConnectString());
        }
        String[] columnNames = ((DefaultClassWriter)codeGenerator.getClassWriter()).getColumnNames();
        DBOutputFormat.setOutput(job, options.getTableName(), columnNames);
    }

    @Override
    protected void configureReducer(Job job, SqoopxOptions options) throws IOException {
        job.setNumReduceTasks(0);
    }

    @Override
    protected void configureJobConf(Job job, SqoopxOptions options) {
        super.configureJobConf(job, options);
        job.getConfiguration().set(ConfigurationConstants.SQOOPX_EXPORT_TABLE_CLASS, this.codeGenerator.getGenerateClass().getName());
    }

    @Override
    protected void cacheJars(Job job, SqoopxOptions options) throws IOException {
        super.cacheJars(job, options);
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.getLocal(conf);
        Set<String> localUrls = new HashSet<String>();

        addToCache(Jars.getJarPathForClass(HCatBaseInputFormat.class), fs, localUrls);
        addToCache(Jars.getJarPathForClass(TypeInfo.class), fs, localUrls);
        addToCache(Jars.getJarPathForClass(HiveStorageHandler.class), fs, localUrls);
        addToCache(Jars.getJarPathForClass(Table.class), fs, localUrls);

        setTmpJarsProperty(conf, localUrls);
    }
}
