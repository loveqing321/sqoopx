package com.deppon.hadoop.sqoopx.core.util.password;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by meepai on 2017/6/23.
 */
public interface PasswordLoader {

    /**
     * 加载密码
     * @param path
     * @param conf
     * @return
     */
    String loadPassword(String path, Configuration conf) throws IOException;

    /**
     * 清除敏感的属性
     * @param conf
     */
    void cleanUpSensitiveProperties(Configuration conf);

}
