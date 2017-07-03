package com.deppon.hadoop.sqoopx.core.codegen;

import com.deppon.hadoop.sqoopx.core.conf.ConfigurationConstants;
import com.deppon.hadoop.sqoopx.core.exception.CodeGenerateException;
import com.deppon.hadoop.sqoopx.core.exception.ParseException;
import com.deppon.hadoop.sqoopx.core.mapreduce.ExportRecordMapper;
import com.deppon.hadoop.sqoopx.core.util.JarUtils;
import com.deppon.hadoop.sqoopx.core.util.LoggingUtils;
import com.google.common.base.Preconditions;
import javassist.CannotCompileException;
import javassist.NotFoundException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by meepai on 2017/6/25.
 */
public class CodeGenerator {

    /**
     * 依赖包
     */
    private Set<String> packageDependencies = new HashSet<String>();

    /**
     * 依赖类
     */
    private Set<String> classDependencies = new HashSet<String>();

    /**
     * 输出类
     */
    private Class generateClass;

    /**
     * 输出jar文件
     */
    private String outJar;

    /**
     * 类的构建起
     */
    private ClassWriter classWriter;


    public CodeGenerator(ClassWriter classWriter){
        this.classWriter = classWriter;
    }

    /**
     * 生成代码
     */
    public void generateJar() throws CodeGenerateException {
        Preconditions.checkNotNull(outJar);
        try {
            // 重新分析依赖
            this.analyseDependencies();
            // 生成类
            this.generateClass();
            // 生成jar包
            this.doGenerateJar();
        } catch (Exception e) {
            throw new CodeGenerateException(e);
        } finally {
            this.clearUp();
        }
    }

    /**
     * 添加包依赖
     * @param pkg
     */
    public void addPackage(Package pkg){
        addPackage(pkg.getName());
    }

    /**
     * 添加包依赖
     * @param pkgName
     */
    public void addPackage(String pkgName){
        packageDependencies.add(pkgName);
    }

    /**
     * 添加类依赖
     * @param clazz
     */
    public void addClass(Class clazz){
        addClass(clazz.getName());
    }

    /**
     * 添加类依赖
     * @param className
     */
    public void addClass(String className){
        classDependencies.add(className);
    }

    /**
     * 将所有依赖的包，解析成依赖类的形式
     */
    protected void analyseDependencies(){
        Set<Class<?>> classes;
        for(String pkg : packageDependencies){
            classes = JarUtils.getClasses(pkg);
            for(Class<?> clazz : classes){
                classDependencies.add(clazz.getName());
            }
        }
    }

    /**
     * 生成Class
     * @throws CodeGenerateException
     */
    private void generateClass() throws CodeGenerateException {
        try {
            this.generateClass = classWriter.writeClassToPool().toClass();
        } catch (Exception e) {
            throw new CodeGenerateException(e);
        }
    }

    /**
     * 构建jar包
     */
    private void doGenerateJar() throws NotFoundException, CannotCompileException, IOException {
        String[] dependencies = this.classDependencies.toArray(new String[0]);
        String[] classes = new String[this.classDependencies.size() + 1];
        System.arraycopy(dependencies, 0, classes, 0, dependencies.length);
        classes[dependencies.length] = this.generateClass.getName();
        JarUtils.jar(classWriter.getPool(), classes, outJar);
    }

    protected void clearUp(){
        this.classWriter.clearPool();
    }

    public Set<String> getPackageDependencies() {
        return packageDependencies;
    }

    public Set<String> getClassDependencies() {
        return classDependencies;
    }

    public Class getGenerateClass() {
        return generateClass;
    }

    public ClassWriter getClassWriter() {
        return classWriter;
    }

    public String getOutJar() {
        return outJar;
    }

    public void setOutJar(String outJar) {
        this.outJar = outJar;
    }

    /**
     * @param classWriter
     * @param jarFile
     * @return
     */
    public static CodeGenerator generateOrmJar(ClassWriter classWriter, String jarFile) throws CodeGenerateException {
        CodeGenerator generator = new CodeGenerator(classWriter);
        generator.setOutJar(jarFile);
        // 设置依赖的类和包
        generator.addPackage(ParseException.class.getPackage());
        generator.addPackage(ExportRecordMapper.class.getPackage());
        generator.addPackage(ConfigurationConstants.class.getPackage());
        generator.addPackage(LoggingUtils.class.getPackage());
        generator.generateJar();
        return generator;
    }
}
