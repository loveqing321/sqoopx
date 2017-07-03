package com.deppon.hadoop.sqoopx.core.tools.job;

import com.deppon.hadoop.sqoopx.core.codegen.JdbcClassWriter;
import com.deppon.hadoop.sqoopx.core.conf.ConfigurationConstants;
import com.deppon.hadoop.sqoopx.core.conf.ConfigurationHelper;
import com.deppon.hadoop.sqoopx.core.file.FileLayout;
import com.deppon.hadoop.sqoopx.core.mapreduce.ExportRecordMapper;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.util.FileSystemUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by meepai on 2017/6/25.
 */
public class HdfsExportJob extends BaseJarJob {

    public HdfsExportJob(){
        super("sqoopx-hdfs-export-");
    }

    @Override
    protected void doRun(SqoopxJobContext context) throws Exception {
        // 配置并执行job
        this.configureJobAndRun(context.getOptions().getConf(), context.getOptions(), codeGenerator.getOutJar());
    }

    @Override
    protected void configureInputFormat(Job job, SqoopxOptions options) throws IOException {
        // 分为几种情况处理，  1. 处理hcatalog  2. 处理文件类型为AVRO  3. 处理文件类型为PARQUET
        Path path = new Path(options.getExportDir());
        try {
            FileInputFormat.addInputPath(job, FileSystemUtils.makeQualified(path, job.getConfiguration()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 处理hcatalog
        if(options.getHCatTableName() != null){

        } else if(options.getFileLayout() == FileLayout.AvroFile){

        } else if(options.getFileLayout() == FileLayout.ParquetFile){

        }
    }

    @Override
    protected void configureMapper(Job job, SqoopxOptions options) throws IOException {
        // 设置mapper的任务数量
        ConfigurationHelper.setJobMapTasks(job, 4);
        // 配置mapper
        job.setMapperClass(ExportRecordMapper.class);
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
        String[] columnNames = ((JdbcClassWriter)codeGenerator.getClassWriter()).getColumnNames();
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
}
