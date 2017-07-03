package com.deppon.hadoop.sqoopx.core.tools.job;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.tools.SqoopxTool;

/**
 * 任务执行上下文
 * Created by meepai on 2017/6/24.
 */
public class SqoopxJobContext {

    private SqoopxOptions options;

    private SqoopxTool sqoopxTool;

    public SqoopxJobContext(SqoopxOptions options, SqoopxTool sqoopxTool) {
        this.options = options;
        this.sqoopxTool = sqoopxTool;
    }

    public SqoopxOptions getOptions() {
        return options;
    }

    public void setOptions(SqoopxOptions options) {
        this.options = options;
    }

    public SqoopxTool getSqoopxTool() {
        return sqoopxTool;
    }

    public void setSqoopxTool(SqoopxTool sqoopxTool) {
        this.sqoopxTool = sqoopxTool;
    }
}
