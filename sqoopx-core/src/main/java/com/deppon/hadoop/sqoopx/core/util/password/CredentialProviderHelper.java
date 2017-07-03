package com.deppon.hadoop.sqoopx.core.util.password;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import java.io.IOException;
import java.util.List;

/**
 * 认证提供者的辅助类，用于从配置中解析指定配置类，来辅助解析。
 * Created by meepai on 2017/6/23.
 */
public final class CredentialProviderHelper {

    /**
     * 处理密码别名 使用hadoop的认证提供者机制  hadoop的认证提供者需要使用 ServiceLoader来加载提供者工厂。
     * @param conf
     * @param alias
     * @return
     */
    public static String resolveAlias(Configuration conf, String alias) throws IOException {
        char[] credentials = conf.getPassword(alias);
        if(credentials == null){
            throw new IOException("The provider alias cannot be resolved, please check the CredentialProviderFactory And property[hadoop.security.credential.provider.path]");
        }
        return new String(credentials);
    }

    /**
     * 创建认证信息
     * @param conf
     * @param alias
     * @param credential
     * @throws IOException
     */
    public static void createCredentialEntry(Configuration conf, String alias, String credential) throws IOException {
        List<CredentialProvider> providers = CredentialProviderFactory.getProviders(conf);
        if(providers != null && providers.size() > 0){
            CredentialProvider provider = providers.get(0);
            provider.createCredentialEntry(alias, credential.toCharArray());
            provider.flush();
        } else {
            throw new IOException("Cannot find credential provider!");
        }
    }
}
