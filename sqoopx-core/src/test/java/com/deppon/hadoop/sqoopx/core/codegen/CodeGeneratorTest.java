package com.deppon.hadoop.sqoopx.core.codegen;

import com.deppon.hadoop.sqoopx.core.exception.CodeGenerateException;
import com.deppon.hadoop.sqoopx.core.metadata.MetadataManager;
import com.deppon.hadoop.sqoopx.core.metadata.jdbc.SqlMetadataManager;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by meepai on 2017/6/25.
 */
public class CodeGeneratorTest {

    public static void main(String[] args){
        SqoopxOptions options = new SqoopxOptions();
        options.setConf(new Configuration());
        options.setConnectString("jdbc:mysql://localhost:3306/test?characterEncoding=utf8&useSSL=true");
        options.setUsername("root");
        options.setPassword("123456");
        options.setHiveTableName("SYS_USER");

        options.setTableName("sys_user");
        options.contribute();

        MetadataManager connManager = new SqlMetadataManager(options);

        ClassWriter classWriter = new DefaultClassWriter(options, connManager);

        try {
            CodeGenerator.generateOrmJar(classWriter, "/Users/meepai/tmp/a1.jar");
        } catch (CodeGenerateException e) {
            e.printStackTrace();
        }

    }
}
