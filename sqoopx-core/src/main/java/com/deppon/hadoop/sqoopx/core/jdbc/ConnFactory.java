package com.deppon.hadoop.sqoopx.core.jdbc;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;

/**
 * Created by meepai on 2017/6/24.
 */
public class ConnFactory {

    private SqoopxOptions options;

    public ConnFactory(SqoopxOptions options){
        this.options = options;
    }

    /**
     * 获取连接管理器
     * @return
     */
    public ConnManager getManager(){
        ConnManager manager = null;
        DBType type = DBType.from(this.options);
        if(type != null){
            switch(type){
                case MYSQL:
                    manager = new MysqlConnManager(this.options);
                    break;
                case ORACLE:
                    manager = new OracleConnManager(this.options);
                    break;
                case POSTGRES:
                    manager = new GreeplumConnManager(this.options);
                    break;
                // TODO 其他数据库暂不写
            }
        }
        return manager;
    }
}
