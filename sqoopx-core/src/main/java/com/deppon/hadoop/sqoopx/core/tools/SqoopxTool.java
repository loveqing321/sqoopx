package com.deppon.hadoop.sqoopx.core.tools;

import com.deppon.hadoop.sqoopx.core.cli.ToolOptions;
import com.deppon.hadoop.sqoopx.core.exception.InvalidOptionsException;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.util.TreeMap;

/**
 * Sqoopx工具类基类，维护一个工具的注册表。
 * Created by meepai on 2017/6/19.
 */
public abstract class SqoopxTool {

    private static final Logger log = Logger.getLogger(SqoopxTool.class);

    private static final TreeMap<String, Class<? extends SqoopxTool>> TOOLS = new TreeMap<String, Class<? extends SqoopxTool>>();

    private static final TreeMap<String, String> TOOLS_DESC = new TreeMap<String, String>();

    protected String toolName;

    static {
        registerTool("codegen", CodeGenTool.class, "export data from hadoop to external data source!");
        registerTool("import", ImportTool.class, "import data from external data source to hadoop!");
        registerTool("export", ExportTool.class, "export data from hadoop to external data source!");
    }

    /**
     * 注册工具
     * @param toolDesc
     */
    public static void registerTool(ToolDesc toolDesc){
        registerTool(toolDesc.getName(), toolDesc.getClazz(), toolDesc.getDesc());
    }

    /**
     * 注册工具
     * @param name
     * @param sqoopxTool
     * @param desc
     */
    public static void registerTool(String name, Class<? extends SqoopxTool> sqoopxTool, String desc){
        Preconditions.checkArgument(name != null, "the register tool name cannot be null!");
        Preconditions.checkArgument(sqoopxTool != null, "the register tool cannot be null!");
        TOOLS.put(name, sqoopxTool);
        TOOLS_DESC.put(name, desc);
    }

    /**
     * 获取已注册的工具
     * @param toolName
     * @return
     */
    public static SqoopxTool getTool(String toolName){
        Class<? extends SqoopxTool> clazz = TOOLS.get(toolName);
        if(clazz != null){
            try {
                SqoopxTool tool = clazz.newInstance();
                tool.setToolName(toolName);
                return tool;
            } catch (InstantiationException e) {
                log.error(e.getMessage(), e);
            } catch (IllegalAccessException e) {
                log.error(e.getMessage(), e);
            }
        }
        return null;
    }

    public String getToolName() {
        return toolName;
    }

    public void setToolName(String toolName) {
        this.toolName = toolName;
    }

    /**
     * 不同的工具类解析参数获取SqoopxOptions对象
     * @param args
     * @param configuration
     * @param options
     * @param useGenericOptions
     * @return
     */
    public abstract SqoopxOptions parseArguments(String[] args, Configuration configuration, SqoopxOptions options, boolean useGenericOptions) throws ParseException, InvalidOptionsException;

    /**
     * 验证Options
     * @param options
     * @throws InvalidOptionsException
     */
    public abstract void validateOptions(SqoopxOptions options) throws InvalidOptionsException;

    /**
     * 配置命令行到Options
     * @param commandLine
     * @param options
     * @throws InvalidOptionsException
     */
    public abstract void applyOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException;

    /**
     * 构建
     * @param options
     */
    public abstract void configureOptions(ToolOptions options);

    /**
     * 运行方法
     * @param options
     * @return
     */
    public abstract int run(SqoopxOptions options) throws Exception;
}
