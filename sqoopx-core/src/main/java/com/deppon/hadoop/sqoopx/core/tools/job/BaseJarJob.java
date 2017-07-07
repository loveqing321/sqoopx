package com.deppon.hadoop.sqoopx.core.tools.job;

import com.deppon.hadoop.sqoopx.core.codegen.ClassWriter;
import com.deppon.hadoop.sqoopx.core.codegen.CodeGenerator;
import com.deppon.hadoop.sqoopx.core.codegen.DefaultClassWriter;
import com.deppon.hadoop.sqoopx.core.metadata.MetadataManager;

import java.io.File;
import java.io.IOException;

/**
 * 基于生成的mapreduce jar的任务
 * Created by meepai on 2017/6/30.
 */
public abstract class BaseJarJob extends BaseJob {

    public static final String DEFAULT_JAR_PREFIX = "sqoopx-codegen";

    private String jarPrefix = DEFAULT_JAR_PREFIX;

    protected CodeGenerator codeGenerator;

    protected MetadataManager metadataManager;

    public BaseJarJob(){}

    public BaseJarJob(String jarPrefix){
        this.jarPrefix = jarPrefix;
    }

    public void run(SqoopxJobContext context) throws Exception {
        this.metadataManager = context.getMetadataManager();
        String jarFile = context.getOptions().getJarOutputDir() + File.separator + jarPrefix + System.currentTimeMillis() + ".jar";
        ClassWriter classWriter = new DefaultClassWriter(context.getOptions(), metadataManager);
        this.codeGenerator = CodeGenerator.generateOrmJar(classWriter, jarFile);
        try {
            this.doRun(context);
        } finally {
//            this.cleanUp();
        }
    }

    protected abstract void doRun(SqoopxJobContext context) throws Exception;

    @Override
    public void cleanUp() throws IOException {
        File file = new File(codeGenerator.getOutJar());
        if(file.exists()){
            if(!file.delete()){
                file.deleteOnExit();
            }
        }
    }
}
