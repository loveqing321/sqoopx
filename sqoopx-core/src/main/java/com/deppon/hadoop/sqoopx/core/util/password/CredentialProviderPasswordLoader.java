package com.deppon.hadoop.sqoopx.core.util.password;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * 基于认证提供者的密码加载器
 * 从文件中加载的密码只是个密码别名，需要使用指定类来处理获取最终密码
 * Created by meepai on 2017/6/23.
 */
public class CredentialProviderPasswordLoader extends FilePasswordLoader {

    @Override
    public String loadPassword(String p, Configuration conf) throws IOException {
        String alias = super.loadPassword(p, conf);
        return CredentialProviderHelper.resolveAlias(conf, alias);
    }
}
