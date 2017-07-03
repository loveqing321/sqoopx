package com.deppon.hadoop.sqoopx.core.tools.job;

/**
 * Created by meepai on 2017/6/24.
 */
public interface SqoopxJob {

    /**
     * 执行任务
     * @throws Exception
     */
    void run(SqoopxJobContext context) throws Exception;

}
