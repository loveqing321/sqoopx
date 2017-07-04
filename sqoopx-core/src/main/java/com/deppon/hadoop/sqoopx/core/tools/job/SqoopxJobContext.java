package com.deppon.hadoop.sqoopx.core.tools.job;

import com.deppon.hadoop.sqoopx.core.metadata.MetadataManager;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.tools.SqoopxTool;

/**
 * 任务执行上下文
 * Created by meepai on 2017/6/24.
 */
public class SqoopxJobContext {

    private SqoopxOptions options;

    private SqoopxTool sqoopxTool;

    private MetadataManager metadataManager;

    public SqoopxJobContext(SqoopxOptions options, SqoopxTool sqoopxTool, MetadataManager metadataManager) {
        this.options = options;
        this.sqoopxTool = sqoopxTool;
        this.metadataManager = metadataManager;
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

    public MetadataManager getMetadataManager() {
        return metadataManager;
    }

    public void setMetadataManager(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }
}
