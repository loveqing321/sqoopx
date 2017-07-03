package com.deppon.hadoop.sqoopx.core.tools;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import javassist.*;

/**
 * Created by meepai on 2017/6/20.
 */
public class CodeGenTool extends BaseSqoopxTool {

    public CodeGenTool() {
        super(null);
    }

    public int run(SqoopxOptions options) throws Exception {

        ClassPool pool = ClassPool.getDefault();

        CtClass ctClass = pool.makeClass("com.test.Main");
        CtField field = new CtField(pool.get("java.lang.String"), "name", ctClass);
        field.setModifiers(Modifier.PUBLIC);
        ctClass.addMethod(CtNewMethod.setter("setName", field));

        ctClass.writeFile("/Users/meepai/tmp");

        Class clazz = ExportTool.class;

        CtClass ct2 = pool.get(clazz.getName());
        ct2.writeFile("/Users/meepai/tmp");

        return 0;
    }
}
