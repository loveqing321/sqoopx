package com.deppon.hadoop.sqoopx.core.util;

import com.deppon.hadoop.sqoopx.core.jdbc.ConnManager;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.mysql.jdbc.Driver;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

/**
 * Created by meepai on 2017/6/27.
 */
public class Jars {



    /**
     * 获取驱动类所在jar
     * @param options
     * @return
     */
    public static String getDriverClassJar(SqoopxOptions options){
        String driverClass = options.getDriverClassName();
        if(driverClass != null){
            try {
                Class driver = Class.forName(driverClass);
                return getJarPathForClass(driver);
            } catch (ClassNotFoundException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * 获取指定类的文件路径
     * @param clazz
     * @return
     */
    public static String getJarPathForClass(Class clazz){
        String classFile = clazz.getName().replaceAll("\\.", "/") + ".class";
        ClassLoader classLoader = clazz.getClassLoader();
        try {
            for(Enumeration<URL> itr = classLoader.getResources(classFile); itr.hasMoreElements();){
                URL url = itr.nextElement();
                if("jar".equals(url.getProtocol())){
                    String ret = url.getPath();
                    if(ret.startsWith("file:")){
                        ret = ret.substring("file:".length());
                    }
                    ret = ret.replaceAll("\\+", "%2B");
                    ret = URLDecoder.decode(ret, "UTF-8");
                    return ret.replaceAll("!.*$", "");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static void main(String[] args){
        System.out.println(Jars.getJarPathForClass(Driver.class));
    }
}
