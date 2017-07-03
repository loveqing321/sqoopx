package com.deppon.hadoop.sqoopx.core;

import com.deppon.hadoop.sqoopx.core.exception.InvalidOptionsException;
import com.deppon.hadoop.sqoopx.core.exception.SqoopxException;
import com.deppon.hadoop.sqoopx.core.ext.SqoopxExtension;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.tools.SqoopxTool;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Sqoopx 扩展SqoopxExtension，用于导入/导出等任务的前后置操作，
 * 具体执行委托给SqoopxTool来执行。
 * Created by meepai on 2017/6/19.
 */
public class Sqoopx extends Configured implements Tool {

    private static final Logger log = Logger.getLogger(Sqoopx.class);

    private SqoopxTool tool;

    private List<SqoopxExtension> extensions;

    private SqoopxOptions options;

    public Sqoopx(SqoopxTool tool, Configuration conf){
        this(tool, conf, new SqoopxOptions());
    }

    public Sqoopx(SqoopxTool tool, Configuration conf, SqoopxOptions options){
        super(conf);
        this.tool = tool;
        this.options = options;
    }

    /**
     * 执行sqoopx操作
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        Preconditions.checkNotNull(tool);
        try {
            // 解析参数并封装称SqoopxOptions对象
            this.options = this.tool.parseArguments(args, getConf(), this.options, false);
            // 打印
            log.info(this.options);
            // 验证配置项
            this.tool.validateOptions(this.options);
            // 构建选项中的扩展字段
            this.options.contribute();
            // 初始化扩展
            this.initExtensions(this.options);
        } catch (Exception e){
            log.error("sqoopx arguments has problem, " + e.getMessage(), e);
            return 1;
        }
        // 执行前置
        this.doPreExtensions();
        // 执行实际的任务
        int ret = this.tool.run(this.options);
        // 执行后置
        this.doPostExtensions();
        return ret;
    }

    public SqoopxTool getTool() {
        return tool;
    }

    public void setTool(SqoopxTool tool) {
        this.tool = tool;
    }

    /**
     * 初始化扩展项
     */
    private void initExtensions(SqoopxOptions options) throws SqoopxException {
        String extensionClasses = options.getExtensionClasses();
        if(extensionClasses != null) {
            String[] classes = extensionClasses.split(",");
            for(int i=0; i<classes.length; i++){
                Class clazz = SqoopxOptions.getClassByName(classes[i]);
                if(!SqoopxExtension.class.isAssignableFrom(clazz)){
                    throw new InvalidOptionsException("Extension class[" + clazz.getName() + "] must implement SqoopxExtension interface!");
                }
                try {
                    this.addExtension((SqoopxExtension) clazz.newInstance());
                } catch (InstantiationException e) {
                    throw new InvalidOptionsException("Cannot find extension class[" + clazz.getName() + "]");
                } catch (IllegalAccessException e) {
                    throw new InvalidOptionsException("Cannot find extension class[" + clazz.getName() + "]");
                }
            }
        }
    }

    /**
     * 添加扩展
     * @param extension
     */
    public void addExtension(SqoopxExtension extension){
        if(extension == null){
            return;
        }
        if(extensions == null){
            extensions = new ArrayList<SqoopxExtension>();
        }
        extensions.add(extension);
    }

    /**
     * 执行前置扩展
     * @throws SqoopxException
     */
    private void doPreExtensions() throws SqoopxException {
        if(extensions != null && extensions.size() > 0){
            for(int i=0; i<extensions.size(); i++){
                this.extensions.get(i).doPreSqoopx(this);
            }
        }
    }

    /**
     * 执行后置扩展
     * @throws SqoopxException
     */
    private void doPostExtensions() throws SqoopxException {
        if(extensions != null && extensions.size() > 0){
            for(int i=0; i<extensions.size(); i++){
                extensions.get(i).doPostSqoopx(this);
            }
        }
    }
}
