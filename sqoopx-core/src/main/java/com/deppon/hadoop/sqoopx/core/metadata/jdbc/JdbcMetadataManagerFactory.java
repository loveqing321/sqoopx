package com.deppon.hadoop.sqoopx.core.metadata.jdbc;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;

/**
 * Created by meepai on 2017/6/24.
 */
public class JdbcMetadataManagerFactory {

    private SqoopxOptions options;

    public JdbcMetadataManagerFactory(SqoopxOptions options){
        this.options = options;
    }

    /**
     * 获取连接管理器
     * @return
     */
    public JdbcMetadataManager getManager(){
        JdbcMetadataManager manager = null;
        DBType type = DBType.from(this.options);
        if(type != null){
            switch(type){
                case MYSQL:
                    manager = new MysqlMetadataManager(this.options);
                    break;
                case ORACLE:
                    manager = new OracleMetadataManager(this.options);
                    break;
                case POSTGRES:
                    manager = new GreeplumMetadataManager(this.options);
                    break;
                // TODO 其他数据库暂不写
            }
        }
        return manager;
    }
}
