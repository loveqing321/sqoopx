package com.deppon.hadoop.sqoopx.core.codegen;

import com.deppon.hadoop.sqoopx.core.metadata.MetadataManager;
import com.deppon.hadoop.sqoopx.core.metadata.jdbc.SqlMetadataManager;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import javassist.CannotCompileException;
import javassist.CtClass;
import javassist.NotFoundException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by meepai on 2017/7/3.
 */
public class JdbcClassWriterTest {

    public static void main(String[] args) throws IOException, CannotCompileException, NotFoundException {
        SqoopxOptions options = new SqoopxOptions();
        options.setConf(new Configuration());
        options.setConnectString("jdbc:mysql://localhost:3306/sqoopx?characterEncoding=utf8&useSSL=true");
        options.setUsername("root");
        options.setPassword("123456");
        options.setTableName("test_word");
        options.contribute();

        MetadataManager connManager = new SqlMetadataManager(options);
        DefaultClassWriter classWriter = new DefaultClassWriter(options, connManager);

        CtClass ctClass = classWriter.writeClassToPool();
        ctClass.writeFile("/Users/meepai/tmp");

    }
}
