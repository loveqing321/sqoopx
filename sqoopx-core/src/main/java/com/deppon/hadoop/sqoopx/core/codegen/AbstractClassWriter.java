package com.deppon.hadoop.sqoopx.core.codegen;

import javassist.ClassPool;
import javassist.CtClass;

/**
 * Created by meepai on 2017/6/25.
 */
public abstract class AbstractClassWriter implements ClassWriter {

    protected ClassPool classPool = new ClassPool(true);

    protected CtClass generate;

    /**
     * 清除class pool占用的资源
     */
    public void clearPool() {
        if(generate != null){
            generate.detach();
        }
        generate = null;
        classPool = null;
    }

    public ClassPool getPool() {
        return classPool;
    }
}
