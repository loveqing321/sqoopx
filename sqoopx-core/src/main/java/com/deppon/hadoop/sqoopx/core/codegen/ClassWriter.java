package com.deppon.hadoop.sqoopx.core.codegen;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;

import java.io.IOException;

/**
 * 类生成器
 * Created by meepai on 2017/6/25.
 */
public interface ClassWriter {

    /**
     * 生成类，并放入pool
     * @return
     */
    CtClass writeClassToPool() throws NotFoundException, IOException, CannotCompileException;

    /**
     * 从Pool中移除类
     */
    void clearPool();

    /**
     * 获取类池
     * @return
     */
    ClassPool getPool();
}
