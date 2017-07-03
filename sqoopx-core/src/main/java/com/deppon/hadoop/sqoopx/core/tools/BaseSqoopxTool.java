package com.deppon.hadoop.sqoopx.core.tools;

import com.deppon.hadoop.sqoopx.core.cli.ToolOptions;
import com.deppon.hadoop.sqoopx.core.exception.InvalidOptionsException;
import com.deppon.hadoop.sqoopx.core.jdbc.ConnFactory;
import com.deppon.hadoop.sqoopx.core.jdbc.ConnManager;
import com.deppon.hadoop.sqoopx.core.options.OptionsConfigurer;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * 基础的Sqoopx工具类，将与配置相关的任务委托给OptionsConfigurer处理。
 * Created by meepai on 2017/6/19.
 */
public abstract class BaseSqoopxTool extends SqoopxTool {

    protected OptionsConfigurer configurer;

    protected ConnManager connManager;

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
        this.connManager = new ConnFactory(options).getManager();
        if(this.connManager == null){
            throw new RuntimeException("Not support this connect string: " + options.getConnectString());
        } else {
            return true;
        }
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

    public ConnManager getConnManager() {
        return connManager;
    }
}
