package com.deppon.hadoop.sqoopx.core.codegen;

import javassist.*;

import java.io.IOException;

/**
 * Created by meepai on 2017/6/27.
 */
public class ClassWriterTest {

    public static void main(String[] args) throws CannotCompileException, IOException, NotFoundException {
        ClassPool pool = new ClassPool(true);
        pool.importPackage(B.class.getPackage().getName());
        CtClass clazz = pool.makeClass("A");
        CtMethod method = new CtMethod(CtClass.voidType, "readFields", new CtClass[0], clazz);
        method.setBody("{ClassWriterTest.B.write(\"123\");}");
        clazz.addMethod(method);
        clazz.writeFile("/Users/meepai/tmp");
    }

    public static class B {

        public static void write(String str){
            System.out.println(str);
        }
    }
}
