package com.deppon.hadoop.sqoopx.core.tools.job;

import com.deppon.hadoop.sqoopx.core.conf.ConfigurationHelper;
import com.deppon.hadoop.sqoopx.core.exception.SqoopxException;
import com.deppon.hadoop.sqoopx.core.mapreduce.ImportRecordMapper;
import com.deppon.hadoop.sqoopx.core.mapreduce.RawKeyTextOuputFormat;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.util.ClassLoaderStack;
import com.deppon.hadoop.sqoopx.core.util.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 目前是实现到hive的导入
 * Created by meepai on 2017/6/25.
 */
public class HdfsImportJob extends BaseJarJob {

    protected Job job;

    private ClassLoader prevClassLoader;

    protected Path destination;

    protected FileSystem fs;

    public HdfsImportJob(){
        super("sqoopx-hdfs-import-");
    }

    public HdfsImportJob(String prefix){
        super(prefix);
    }

    @Override
    protected void doRun(SqoopxJobContext context) throws Exception {
        SqoopxOptions options = context.getOptions();
        destination = PathUtils.getImportPath(options);
        fs = destination.getFileSystem(options.getConf());
        if(fs.exists(destination)){
            fs.delete(destination, true);
        }
        String tableName = options.getTableName();
        boolean flag;
        // 先将数据导入到hdfs
        if (tableName != null) {
            flag = importTable(options, codeGenerator.getOutJar(), codeGenerator.getGenerateClass());
        } else {
            flag = importQuery(options, codeGenerator.getOutJar(), codeGenerator.getGenerateClass());
        }
        if (!flag) {
            throw new SqoopxException("import task has problem, please check the logs.");
        }
    }

    private boolean importTable(SqoopxOptions options, String ormJarFile, Class tableClass) throws IOException {
        return runImport(options, ormJarFile, tableClass);
    }

    private boolean importQuery(SqoopxOptions options, String ormJarFile, Class tableClass) throws IOException {
        return runImport(options, ormJarFile, tableClass);
    }

    private boolean runImport(SqoopxOptions options, String ormJarFile, Class tableClass) throws IOException {
        Configuration conf = options.getConf();
        // 本地模式需要加载到本地jvm中
        loadJarsInLocal(conf, ormJarFile, tableClass.getName());
        try {
            return this.configureJobAndRun(conf, options, ormJarFile);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if(this.prevClassLoader != null){
                ClassLoaderStack.setCurrentClassLoader(this.prevClassLoader);
            }
        }
    }

    /**
     * @param conf
     * @param ormJarFile
     * @param className
     * @throws IOException
     */
    private void loadJarsInLocal(Configuration conf, String ormJarFile, String className) throws IOException {
        boolean isLocal = "local".equals(conf.get("mapreduce.jobtracker.address")) || "local".equals(conf.get("mapred.job.tracker"));
        if(isLocal){
            this.prevClassLoader = ClassLoaderStack.addJarFile(ormJarFile, className);
        }
    }

    @Override
    protected void configureInputFormat(Job job, SqoopxOptions options) throws IOException {
        job.setInputFormatClass(DBInputFormat.class);
        if(options.getUsername() != null){
            DBConfiguration.configureDB(job.getConfiguration(), options.getDriverClassName(),
                    options.getConnectString(), options.getUsername(), options.getPassword());
        } else {
            DBConfiguration.configureDB(job.getConfiguration(), options.getDriverClassName(),
                    options.getConnectString());
        }
        DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
        // 设置DBInputFormat要ORM的对象
        dbConf.setInputClass(codeGenerator.getGenerateClass());
        if(options.getTableName() != null){
            dbConf.setInputTableName(options.getTableName());
        } else if(options.getSqlQuery() != null){
            dbConf.setInputQuery(options.getSqlQuery());
        }
        dbConf.setInputFieldNames(metadataManager.getColumnNames());
        if(options.getWhereClause() != null){
            dbConf.setInputConditions(options.getWhereClause());
        }
    }

    @Override
    protected void configureMapper(Job job, SqoopxOptions options) throws IOException {
        job.setMapperClass(ImportRecordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        ConfigurationHelper.setJobMapTasks(job, 4);
    }

    @Override
    protected void configureOutputFormat(Job job, SqoopxOptions options) throws IOException {
        job.setOutputFormatClass(RawKeyTextOuputFormat.class);
        FileOutputFormat.setOutputPath(job, destination);
    }

    @Override
    protected void configureReducer(Job job, SqoopxOptions options) throws IOException {

    }

    @Override
    protected void configureJobConf(Job job, SqoopxOptions options) {
        ConfigurationHelper.setJobMapSeculative(job, false);
    }
}
