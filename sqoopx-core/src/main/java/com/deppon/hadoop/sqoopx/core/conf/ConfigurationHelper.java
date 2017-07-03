package com.deppon.hadoop.sqoopx.core.conf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;

import java.io.IOException;

/**
 * Created by meepai on 2017/6/21.
 */
public final class ConfigurationHelper {

    /**
     * job 相关配置
     */

    /**
     * 设置job所要提交的jar文件
     * @param job
     * @param jarFile
     */
    public static void setJobJar(Job job, String jarFile){
        job.getConfiguration().set(ConfigurationConstants.PROP_MAPREDUCE_JOB_JAR, jarFile);
    }

    /**
     * 添加依赖jar包
     * @param job
     * @param jarPath
     * @throws IOException
     */
    public static void addFileToClassPath(Job job, String jarPath) throws IOException {
        Path path = new Path(jarPath);
        DistributedCache.addFileToClassPath(path, job.getConfiguration());
    }

    /**
     * 设置job分配mapper任务的数量
     * @param job
     * @param numMapTasks
     */
    public static void setJobMapTasks(Job job, int numMapTasks){
        // 2.0以前的版本使用  mapred.map.tasks
        int num = job.getConfiguration().getInt(ConfigurationConstants.PROP_MAPREDUCE_JOB_MAPS, 0);
        if(num == 0) {
            job.getConfiguration().setInt(ConfigurationConstants.PROP_MAPREDUCE_JOB_MAPS, numMapTasks);
        }
    }

    /**
     * 获取job分配mapper任务的数量
     * @param context
     * @return
     */
    public static int getJobMapTasks(JobContext context){
        // 2.0以前的版本使用  mapred.map.tasks
        return context.getConfiguration().getInt(ConfigurationConstants.PROP_MAPREDUCE_JOB_MAPS, 1);
    }

    /**
     * 设置是否开启job中map的预测机制
     * 预测机制可能在部分机器资源不足的情况下，产生重复写公共资源（如数据库）
     * @param job
     * @param enabled
     */
    public static void setJobMapSeculative(Job job, boolean enabled){
        // 2.0以前的版本使用 mapred.map.tasks.speculative.execution mapreduce.map.speculative
        job.getConfiguration().setBoolean(ConfigurationConstants.PROP_MAPREDUCE_MAP_SPECULATIVE, enabled);
    }

    /**
     * 设置是否开启job中reduce的预测机制
     * 预测机制可能在部分机器资源不足的情况下，产生重复写公共资源（如数据库）
     * @param job
     * @param enabled
     */
    public static void setJobReduceSeculative(Job job, boolean enabled){
        // 2.0以前的版本使用 mapred.reduce.tasks.speculative.execution
        job.getConfiguration().setBoolean(ConfigurationConstants.PROP_MAPREDUCE_REDUCE_SPECULATIVE, enabled);
    }

    /**
     * 设置jobTracker的地址
     * @param job
     * @param addr
     */
    public static void setJobTrackerAddr(Job job, String addr){
        job.getConfiguration().set(ConfigurationConstants.PROP_MAPREDUCE_JOB_TRACKER_ADDRESS, addr);
    }

    /**
     * 获取job的map任务的输入记录数
     * @param job
     * @return
     * @throws IOException
     */
    public static long getNumMapInputRecords(Job job) throws IOException {
        return job.getCounters().findCounter(ConfigurationConstants.COUNTER_GROUP_MAPRED_TASK_COUNTERS
                , ConfigurationConstants.COUNTER_MAP_INPUT_RECORDS).getValue();
    }

    /**
     * 获取job的map任务的输出记录数
     * @param job
     * @return
     * @throws IOException
     */
    public static long getNumMapOutputRecords(Job job) throws IOException {
        return job.getCounters().findCounter(ConfigurationConstants.COUNTER_GROUP_MAPRED_TASK_COUNTERS
                , ConfigurationConstants.COUNTER_MAP_OUTPUT_RECORDS).getValue();
    }
}
