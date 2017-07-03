package com.deppon.hadoop.sqoopx.core.options;

import com.deppon.hadoop.sqoopx.core.cli.ToolOptions;
import com.deppon.hadoop.sqoopx.core.exception.InvalidOptionsException;
import com.deppon.hadoop.sqoopx.core.tools.SqoopxTool;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

/**
 * Options配置器
 * Created by meepai on 2017/6/19.
 */
public interface OptionsConfigurer {

    /**
     * 不同的工具类解析参数获取SqoopxOptions对象  外部tool调用，用于触发参数解析整个流程。
     * @param args
     * @param configuration
     * @param options
     * @param useGenericOptions
     * @return
     */
    SqoopxOptions parseArguments(String[] args, Configuration configuration, SqoopxOptions options, boolean useGenericOptions) throws ParseException, InvalidOptionsException;

    /**
     * 验证Options  外部tool调用，用于验证Options是否合理
     * @param options
     * @throws InvalidOptionsException
     */
    void validateOptions(SqoopxOptions options) throws InvalidOptionsException;

    /**
     * 配置命令行到Options  内部调用，用于从命令行参数构建SqoopxOptions对象
     * @param commandLine
     * @param options
     * @throws InvalidOptionsException
     */
    void applyOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException;

    /**
     * 构建cli options  内部调用，用于构建 cli Options对象
     * @param options
     */
    void configureOptions(ToolOptions options);

    /**
     * 获取额外的参数
     * @return
     */
    String[] getExtraArguments();
}
