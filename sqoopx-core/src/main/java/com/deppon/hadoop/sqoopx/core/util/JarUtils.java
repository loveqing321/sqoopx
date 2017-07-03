package com.deppon.hadoop.sqoopx.core.util;

import com.deppon.hadoop.sqoopx.core.tools.ToolDesc;
import com.google.common.base.Preconditions;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;

import java.io.*;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.*;
import java.util.zip.ZipEntry;

/**
 * Created by meepai on 2017/6/21.
 */
public class JarUtils {


    public static void jar(ClassPool pool, String[] classes, String jarFilename) throws IOException, NotFoundException, CannotCompileException {
        Preconditions.checkArgument(classes != null);
        Preconditions.checkArgument(jarFilename != null && !"".equals(jarFilename.trim()));
        File file = new File(jarFilename);
        if(file.exists() && file.isFile()){
            file.delete();
        }
        file.createNewFile();
        FileOutputStream fos = new FileOutputStream(jarFilename);
        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        attributes.put(new Attributes.Name("Built-By"), "deppon");
        attributes.put(new Attributes.Name("Build-Jdk"), "1.7.0_67");
        JarOutputStream jos = new JarOutputStream(fos, manifest);
        for(int i=0; i<classes.length; i++){
            createPackageAndWriteClass(pool, classes[i], jos);
        }
        jos.finish();
        jos.close();
    }

    /**
     * 打包一些类到jar文件中
     */
    public static void jar(ClassPool pool, Class[] classes, String jarFilename) throws IOException, NotFoundException, CannotCompileException {
        Preconditions.checkArgument(classes != null);
        Preconditions.checkArgument(jarFilename != null && !"".equals(jarFilename.trim()));
        String[] classNames = new String[classes.length];
        for(int i=0; i<classes.length; i++){
            classNames[i] = classes[i].getName();
        }
        jar(pool, classNames, jarFilename);
    }

    /**
     * 创建包，并写入类
     * @param className
     * @param jos
     * @throws IOException
     * @throws NotFoundException
     * @throws CannotCompileException
     */
    private static void createPackageAndWriteClass(ClassPool pool, String className, JarOutputStream jos) throws IOException, NotFoundException, CannotCompileException {
        String classPath = className.replaceAll("\\.", "/") + ".class";
        jos.putNextEntry(new ZipEntry(classPath));
        jos.write(getClassBytes(pool, className));
        jos.closeEntry();
    }

    /**
     * 获取Class类的字节数据
     * @return
     * @throws NotFoundException
     * @throws IOException
     * @throws CannotCompileException
     */
    public static byte[] getClassBytes(ClassPool pool, String className) throws NotFoundException, IOException, CannotCompileException {
        CtClass ctClass = null;
        if(pool == null){
            try {
                ctClass = ClassPool.getDefault().get(className);
                return ctClass.toBytecode();
            } finally {
                if(ctClass != null){
                    ctClass.detach();
                }
            }
        } else {
            ctClass = pool.get(className);
            return ctClass.toBytecode();
        }
    }

    /**
     * 打包一些类到jar文件中
     */
    public static void jarPkg(ClassPool pool, String[] packages, String jarFilename) throws IOException, NotFoundException, CannotCompileException {
        Set<Class> classes = new HashSet<Class>();
        for(int i=0; i<packages.length; i++){
            classes.addAll(getClasses(packages[i]));
        }
        jar(pool, classes.toArray(new Class[classes.size()]), jarFilename);
    }

    /**
     * 从包package中获取所有的Class
     * @param packageName
     * @return
     */
    public static Set<Class<?>> getClasses(String packageName){
        //第一个class类的集合
        Set<Class<?>> classes = new HashSet<Class<?>>();
        //是否循环迭代
        boolean recursive = true;
        //获取包的名字 并进行替换
        String packageDirName = packageName.replace('.', '/');
        //定义一个枚举的集合 并进行循环来处理这个目录下的things
        Enumeration<URL> dirs;
        try {
            dirs = Thread.currentThread().getContextClassLoader().getResources(packageDirName);
            //循环迭代下去
            while (dirs.hasMoreElements()){
                //获取下一个元素
                URL url = dirs.nextElement();
                //得到协议的名称
                String protocol = url.getProtocol();
                //如果是以文件的形式保存在服务器上
                if ("file".equals(protocol)) {
                    //获取包的物理路径
                    String filePath = URLDecoder.decode(url.getFile(), "UTF-8");
                    //以文件的方式扫描整个包下的文件 并添加到集合中
                    findAndAddClassesInPackageByFile(packageName, filePath, recursive, classes);
                } else if ("jar".equals(protocol)){
                    //如果是jar包文件
                    //定义一个JarFile
                    JarFile jar;
                    try {
                        //获取jar
                        jar = ((JarURLConnection) url.openConnection()).getJarFile();
                        //从此jar包 得到一个枚举类
                        Enumeration<JarEntry> entries = jar.entries();
                        //同样的进行循环迭代
                        while (entries.hasMoreElements()) {
                            //获取jar里的一个实体 可以是目录 和一些jar包里的其他文件 如META-INF等文件
                            JarEntry entry = entries.nextElement();
                            String name = entry.getName();
                            //如果是以/开头的
                            if (name.charAt(0) == '/') {
                                //获取后面的字符串
                                name = name.substring(1);
                            }
                            //如果前半部分和定义的包名相同
                            if (name.startsWith(packageDirName)) {
                                int idx = name.lastIndexOf('/');
                                //如果以"/"结尾 是一个包
                                if (idx != -1) {
                                    //获取包名 把"/"替换成"."
                                    packageName = name.substring(0, idx).replace('/', '.');
                                }
                                //如果可以迭代下去 并且是一个包
                                if ((idx != -1) || recursive){
                                    //如果是一个.class文件 而且不是目录
                                    if (name.endsWith(".class") && !entry.isDirectory()) {
                                        //去掉后面的".class" 获取真正的类名
                                        String className = name.substring(packageName.length() + 1, name.length() - 6);
                                        try {
                                            //添加到classes
                                            classes.add(Class.forName(packageName + '.' + className));
                                        } catch (ClassNotFoundException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return classes;
    }

    /**
     * 以文件的形式来获取包下的所有Class
     * @param packageName
     * @param packagePath
     * @param recursive
     * @param classes
     */
    public static void findAndAddClassesInPackageByFile(String packageName, String packagePath, final boolean recursive, Set<Class<?>> classes){
        //获取此包的目录 建立一个File
        File dir = new File(packagePath);
        //如果不存在或者 也不是目录就直接返回
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }
        //如果存在 就获取包下的所有文件 包括目录
        File[] dirfiles = dir.listFiles(new FileFilter() {
            //自定义过滤规则 如果可以循环(包含子目录) 或则是以.class结尾的文件(编译好的java类文件)
            public boolean accept(File file) {
                return (recursive && file.isDirectory()) || (file.getName().endsWith(".class"));
            }
        });
        //循环所有文件
        for (File file : dirfiles) {
            //如果是目录 则继续扫描
            if (file.isDirectory()) {
                findAndAddClassesInPackageByFile(packageName + "." + file.getName(),
                        file.getAbsolutePath(),
                        recursive,
                        classes);
            }
            else {
                //如果是java类文件 去掉后面的.class 只留下类名
                String className = file.getName().substring(0, file.getName().length() - 6);
                try {
                    //添加到集合中去
                    classes.add(Class.forName(packageName + '.' + className));
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args){
        try {
            JarUtils.jar(null, new Class[]{FileUtils.class}, "/Users/meepai/tmp/ss4.jar");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CannotCompileException e) {
            e.printStackTrace();
        } catch (NotFoundException e) {
            e.printStackTrace();
        }
        try {
            JarUtils.jarPkg(null, new String[]{ToolDesc.class.getPackage().getName()}, "/Users/meepai/tmp/ss6.jar");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CannotCompileException e) {
            e.printStackTrace();
        } catch (NotFoundException e) {
            e.printStackTrace();
        }
    }
}
