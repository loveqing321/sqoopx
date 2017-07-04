package com.deppon.hadoop.sqoopx.core.tools;

import com.deppon.hadoop.sqoopx.core.cli.ToolOptions;
import com.deppon.hadoop.sqoopx.core.exception.InvalidOptionsException;
import com.deppon.hadoop.sqoopx.core.metadata.MetadataManager;
import com.deppon.hadoop.sqoopx.core.options.OptionsConfigurer;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.tools.job.SqoopxJobContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

/**
 * 基础的Sqoopx工具类，将与配置相关的任务委托给OptionsConfigurer处理。
 * Created by meepai on 2017/6/19.
 */
public abstract class BaseSqoopxTool extends SqoopxTool {

    protected MetadataManager metadataManager;

    protected OptionsConfigurer configurer;

    public BaseSqoopxTool(OptionsConfigurer configurer){
        this.configurer = configurer;
    }

    /**
     * 每个子类需要调用该方法来进行 sqoopx工具的初始化操作
     * @param options
     * @return
     */
    protected boolean init(SqoopxOptions options) {
        options.setSqoopxTool(this);
        return true;
    }

    /**
     * 构建任务执行上下文
     * @param options
     * @return
     */
    protected SqoopxJobContext buildContext(SqoopxOptions options){
        return new SqoopxJobContext(options, this, metadataManager);
    }

    @Override
    public SqoopxOptions parseArguments(String[] args, Configuration configuration, SqoopxOptions options, boolean useGenericOptions) throws ParseException, InvalidOptionsException {
        if(this.configurer != null) {
            return configurer.parseArguments(args, configuration, options, useGenericOptions);
        }
        return options;
    }

    @Override
    public void validateOptions(SqoopxOptions options) throws InvalidOptionsException {
        if(this.configurer != null) {
            configurer.validateOptions(options);
        }
    }

    @Override
    public void applyOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException {
        if(this.configurer != null) {
            configurer.applyOptions(commandLine, options);
        }
    }

    @Override
    public void configureOptions(ToolOptions options) {
        if(this.configurer != null) {
            configurer.configureOptions(options);
        }
    }

    public MetadataManager getMetadataManager() {
        return metadataManager;
    }

    public void setMetadataManager(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }
}
