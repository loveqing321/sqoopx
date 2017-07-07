package com.deppon.hadoop.sqoopx.core.tools.job;

import com.deppon.hadoop.sqoopx.core.conf.ConfigurationConstants;
import com.deppon.hadoop.sqoopx.core.conf.ConfigurationHelper;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.util.Jars;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by meepai on 2017/6/24.
 */
public abstract class BaseJob implements SqoopxJob {

    /**
     * 配置任务并执行 门面方法
     * @param conf
     * @param options
     * @param ormJarFile
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    protected boolean configureJobAndRun(Configuration conf, SqoopxOptions options, String ormJarFile) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf);
        // 1.将options中的属性传播到job中
        this.propagateOptionsToJob(options, job);
        // 2.配置jar和依赖
        this.configureJarFile(job, ormJarFile);
        // 3.配置InputFormat
        this.configureInputFormat(job, options);
        // 4.配置Mapper类和输出类型
        this.configureMapper(job, options);
        // 5.配置Reducer类
        this.configureReducer(job, options);
        // 6.配置OutputFormat
        this.configureOutputFormat(job, options);
        // 7.配置其他属性 像：关闭预测模式等。
        this.configureJobConf(job, options);
        // 8.缓存依赖jar
        this.cacheJars(job, options);
        // 9.执行job
        return this.runJob(job);
    }

    /**
     * 1.将options中的属性传播到job中
     * @param options
     * @param job
     */
    protected void propagateOptionsToJob(SqoopxOptions options, Job job){
        Configuration conf = job.getConfiguration();
        Properties properties = options.writeProperties();
        for(Map.Entry entry : properties.entrySet()){
            conf.set(entry.getKey().toString(), entry.getValue().toString());
        }
    }

    /**
     * 2.配置jar和依赖
     * @param job
     * @param ormJarFile
     */
    protected void configureJarFile(Job job, String ormJarFile){
        ConfigurationHelper.setJobJar(job, ormJarFile);
    }

    /**
     * 3.配置InputFormat
     * @param job
     */
    protected void configureInputFormat(Job job, SqoopxOptions options) throws IOException {

    }

    /**
     * 4.配置Mapper类和输出类型
     * @param job
     */
    protected void configureMapper(Job job, SqoopxOptions options) throws IOException {

    }

    /**
     * 5.配置Mapper类和输出类型
     * @param job
     */
    protected void configureReducer(Job job, SqoopxOptions options) throws IOException {

    }

    /**
     * 6.配置OutputFormat
     * @param job
     */
    protected void configureOutputFormat(Job job, SqoopxOptions options) throws IOException {

    }

    /**
     * 7.配置其他属性 像：关闭预测模式等。
     * @param job
     */
    protected void configureJobConf(Job job, SqoopxOptions options){
        ConfigurationHelper.setJobMapSeculative(job, false);
    }

    /**
     * 8.增加依赖。
     * 缓存依赖jar
     */
    protected void cacheJars(Job job, SqoopxOptions options) throws IOException {
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.getLocal(conf);
        Set<String> localUrls = new HashSet<String>();
        // 添加依赖
        // 数据库驱动
        addToCache(Jars.getDriverClassJar(options), fs, localUrls);
        // 设置tmpjars
        setTmpJarsProperty(conf, localUrls);
    }



    /**
     * 8.执行job
     * @param job
     * @return
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    protected boolean runJob(Job job) throws InterruptedException, IOException, ClassNotFoundException {
        return job.waitForCompletion(true);
    }

    /**
     * 执行清理工作
     */
    public void cleanUp() throws IOException {}

    /**
     * 设置tmpjars属性
     * @param conf
     * @param localUrls
     */
    protected void setTmpJarsProperty(Configuration conf, Set<String> localUrls){
        String tmpjars = conf.get(ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM);
        StringBuilder sb = new StringBuilder();
        if(localUrls.isEmpty()){
            return;
        }
        if(tmpjars != null){
            String[] elements = tmpjars.split(",");
            for(String element : elements){
                if(!element.isEmpty()){
                    sb.append(element);
                    sb.append(",");
                }
            }
        }
        sb.append(StringUtils.arrayToString(localUrls.toArray(new String[0])));
        conf.set(ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM, sb.toString());
    }

    /**
     * 将文件转换成文件系统路径，并存储在集合中
     * @param file
     * @param fs
     * @param localUrls
     */
    protected void addToCache(String file, FileSystem fs, Set<String> localUrls){
        if(file == null){
            return;
        }
        Path path = new Path(file);
        String qualified = fs.makeQualified(path).toString();
        localUrls.add(qualified);
    }

    /**
     * 添加目录到Cache
     * @param dir
     * @param fs
     * @param localUrls
     */
    protected void addDirToCache(File dir, FileSystem fs, Set<String> localUrls, boolean recursive){
        if(dir == null){
            return;
        }
        for(File file : dir.listFiles()){
            if(file.exists()){
                if(file.isDirectory() && recursive){
                    addDirToCache(file, fs, localUrls, recursive);
                } else if(file.getName().endsWith("jar")){
                    addToCache(file.toString(), fs, localUrls);
                }
            }
        }
    }
}
