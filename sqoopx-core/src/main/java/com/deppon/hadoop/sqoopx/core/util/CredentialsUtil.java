package com.deppon.hadoop.sqoopx.core.util;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.util.password.FilePasswordLoader;
import com.deppon.hadoop.sqoopx.core.util.password.PasswordLoader;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by meepai on 2017/6/23.
 */
public final class CredentialsUtil {

    private static final String PROPERTY_LOADER_CLASS = "com.deppon.sqoopx.credentials.loader.class";

    private static final String DEFAULT_PASSWORD_LOADER = FilePasswordLoader.class.getName();

    /**
     * 获取密码
     * @param options
     * @return
     * @throws IOException
     */
    public static String fetchPassword(SqoopxOptions options) throws IOException {
        String passwordFilePath = options.getPasswordFilePath();
        if(passwordFilePath == null){
            return options.getPassword();
        }
        return fetchPasswordFromLoader(passwordFilePath, options.getConf());
    }

    /**
     * 使用PasswordLoader来加载密码
     * @param filePath
     * @param conf
     * @return
     * @throws IOException
     */
    public static String fetchPasswordFromLoader(String filePath, Configuration conf) throws IOException{
        PasswordLoader loader = getLoader(conf);
        return loader.loadPassword(filePath, conf);
    }

    /**
     * 获取PasswordLoader
     * @param conf
     * @return
     * @throws IOException
     */
    public static PasswordLoader getLoader(Configuration conf) throws IOException {
        String loaderClass = conf.get(PROPERTY_LOADER_CLASS, DEFAULT_PASSWORD_LOADER);
        try {
            return (PasswordLoader) Class.forName(loaderClass).newInstance();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
