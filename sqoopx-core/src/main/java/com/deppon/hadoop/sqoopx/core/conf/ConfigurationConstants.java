package com.deppon.hadoop.sqoopx.core.conf;

import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * 配置参数常量
 * Created by meepai on 2017/6/19.
 */
public final class ConfigurationConstants {


    /**
     * job tags属性
     */
    public static final String MR_JOB_TAGS = MRJobConfig.JOB_TAGS;


    /**
     * mapreduce credentials文件
     */
    public static final String MR_JOB_CREDENTIALS_BINARY = MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY;

    /**
     * mapreduce依赖jar
     */
    public static final String MAPRED_DISTCACHE_CONF_PARAM = "tmpjars";

    /**
     * 导出的类名
     */
    public static final String SQOOPX_EXPORT_TABLE_CLASS = "sqoopx.mapreduce.table.class";

    /**
     * 指定sqoopx启动的job id
     */
    public static final String LAUNCHER_JOB_ID = "sqoopx.launcher.job.id";

    /**
     * 指定sqoopx启动的子任务标签
     */
    public static final String LAUNCHER_CHILD_JOB_TAG = "sqoopx.launcher.child.job.tag";

    /**
     * 配置
     */
    public static final String LAUNCHER_JOB_LOGS_DIR = "sqoopx.launcher.job.logs.dir";

    /**
     * jar文件输出目录
     */
    public static final String JAR_OUTPUT_DIR = "sqoopx.jar.output.dir";

    /**
     * 临时hdfs目录，用于数据临时存储Append模式
     */
    public static final String TEMPORARY_ROOT_DIR = "sqoopx.temporary.root.dir";

    /**
     * 临时的本地目录，用于脚本输出
     */
    public static final String TEMPORARY_DIR = "sqoopx.temporary.dir";

    /**
     * mapreduce job提交jar参数
     */
    public static final String PROP_MAPREDUCE_JOB_JAR = "mapreduce.job.jar";

    /**
     * 当前任务id.
     */
    public static final String PROP_MAPRED_TASK_ID = "mapred.task.id";

    /**
     * job本地目录.
     */
    public static final String PROP_JOB_LOCAL_DIRECTORY = "job.local.dir";

    /**
     * map任务的数量  (old).
     */
    public static final String PROP_MAPRED_MAP_TASKS = "mapred.map.tasks";

    /**
     * map任务的数量  (new).
     */
    public static final String PROP_MAPREDUCE_JOB_MAPS = "mapreduce.job.maps";

    /**
     * map任务的预测机制开启属性 (old)
     * 预测机制可能在部分机器资源不足的情况下，产生重复写公共资源（如数据库）
     */
    public static final String PROP_MAPRED_MAP_TASKS_SPECULATIVE_EXEC = "mapred.map.tasks.speculative.execution";

    /**
     * map任务的预测机制开启属性 (new)
     * 预测机制可能在部分机器资源不足的情况下，产生重复写公共资源（如数据库）
     */
    public static final String PROP_MAPREDUCE_MAP_SPECULATIVE = "mapreduce.map.speculative";

    /**
     * reduce任务的预测机制开启属性 (old)
     * 预测机制可能在部分机器资源不足的情况下，产生重复写公共资源（如数据库）
     */
    public static final String PROP_MAPRED_REDUCE_TASKS_SPECULATIVE_EXEC = "mapred.reduce.tasks.speculative.execution";

    /**
     * reduce任务的预测机制开启属性 (new)
     * 预测机制可能在部分机器资源不足的情况下，产生重复写公共资源（如数据库）
     */
    public static final String PROP_MAPREDUCE_REDUCE_SPECULATIVE = "mapreduce.reduce.speculative";

    /**
     * job tracker地址 (old)
     */
    public static final String PROP_MAPRED_JOB_TRACKER_ADDRESS = "mapred.job.tracker";

    /**
     * job tracker地址 (new).
     */
    public static final String PROP_MAPREDUCE_JOB_TRACKER_ADDRESS = "mapreduce.jobtracker.address";

    /**
     * mapreduce框架选择参数  默认为：local，可选：yarn
     */
    public static final String PROP_MAPREDUCE_FRAMEWORK_NAME = "mapreduce.framework.name";

    /**
     * 任务计数的分组名称
     */
    public static final String COUNTER_GROUP_MAPRED_TASK_COUNTERS = "org.apache.hadoop.mapred.Task$Counter";

    /**
     * map阶段输出记录Counter
     */
    public static final String COUNTER_MAP_OUTPUT_RECORDS = "MAP_OUTPUT_RECORDS";

    /**
     * map阶段输入记录Counter
     */
    public static final String COUNTER_MAP_INPUT_RECORDS = "MAP_INPUT_RECORDS";

}
