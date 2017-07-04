package com.deppon.hadoop.sqoopx.core.metadata.jdbc;

import java.sql.Connection;

/**
 * Created by meepai on 2017/6/23.
 */
public enum JdbcTransactionLevels {

    // 无事务
    TRANSACTION_NONE(Connection.TRANSACTION_NONE),
    // 读未提交  最低级别,无任何保证
    TRANSACTION_READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
    // 读已提交  可避免脏读
    TRANSACTION_READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
    // 可重复读  可避免脏读，不可重复读
    TRANSACTION_REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
    // 串行化  可避免脏读，不可重复读，幻读
    TRANSACTION_SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);

    public final int value;

    JdbcTransactionLevels(int value){
        this.value = value;
    }
}
