package com.deppon.hadoop.sqoopx.core.tools;

import com.deppon.hadoop.sqoopx.core.conf.ConfigurationHelper;
import com.deppon.hadoop.sqoopx.core.mapreduce.*;
import com.deppon.hadoop.sqoopx.core.options.ExportOptionsConfigurer;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.tools.job.HdfsExportJob;
import com.deppon.hadoop.sqoopx.core.tools.job.SqoopxJob;
import com.deppon.hadoop.sqoopx.core.tools.job.SqoopxJobContext;
import javassist.CannotCompileException;
import javassist.NotFoundException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 导出工具  hadoop集群数据导出到外部源
 * 包括：hive数据／Sequence文件数据／Avro文件数据／Parquet文件
 *
 * Created by meepai on 2017/6/19.
 */
public class ExportTool extends BaseSqoopxTool {

    private static final Logger log = Logger.getLogger(ExportTool.class);

    public ExportTool() {
        super(new ExportOptionsConfigurer());
    }

    /**
     * 执行导出功能
     * @param options
     * @return
     * @throws Exception
     */
    public int run(SqoopxOptions options) throws Exception {
        if(!init(options)){
            return 1;
        }
        // 如果指定了要更新的列
        if(options.getUpdateKeyCol() != null){
            // 配置输出列的顺序
//            this.connManager.

            // 如果是只更新模式
            if(options.getUpdateMode() == SqoopxOptions.UpdateMode.UpdateOnly){

            } else {
                // 默认是允许插入的模式
            }
        } else if(options.getCall() != null) {
            // TODO
            log.error("暂没提供存储过程实现！");
        } else {
            SqoopxJob job = new HdfsExportJob();
            SqoopxJobContext context = new SqoopxJobContext(options, this);
            job.run(context);
        }
//        // 生成jar包
//        String jarFile = this.generateJar();
//        // 构建job
//        Job job = this.buildJob(jarFile, options);
//        // job执行
//        job.waitForCompletion(true);
        return 0;
    }

    /**
     * 生成jar
     * @return
     */
    private String generateJar() throws NotFoundException, CannotCompileException, IOException {
        Package package1 = SimpleRecord.class.getPackage();
        Package package2 = ExportRecordMapper.class.getPackage();
        String[] packages = new String[]{package1.getName(), package2.getName()};

        String jarFilename = "/Users/meepai/tmp/job02.jar";
//        JarUtils.jar(packages, jarFilename);
        return jarFilename;
    }

    /**
     * 构建job
     * @param jarFile
     * @return
     */
    private Job buildJob(String jarFile, SqoopxOptions options) throws IOException {
        Job job = Job.getInstance(options.getConf());
        // 设置其他参数
        job.getConfiguration().setBoolean("sqoopx.verbose", options.isVerbose());
        // 设置jar
        ConfigurationHelper.setJobJar(job, jarFile);
        // 设置mapper的任务数量
        ConfigurationHelper.setJobMapTasks(job, 4);
        // 配置mapper
        job.setMapperClass(ExportRecordMapper.class);
        // 设置mapper的输出类型
        job.setMapOutputKeyClass(Record.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 设置InputFormat
        FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/test/input/1.txt"));
        // 设置job的输出格式为NullOutputFormat  否则默认为FileOutputFormat
        job.setOutputFormatClass(NullOutputFormat.class);
        // 设置关闭job的map任务的预测机制
        ConfigurationHelper.setJobMapSeculative(job, false);
        return job;
    }
}
