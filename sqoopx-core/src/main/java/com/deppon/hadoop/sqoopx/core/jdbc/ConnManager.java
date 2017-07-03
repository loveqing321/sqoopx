package com.deppon.hadoop.sqoopx.core.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 *
 * Created by meepai on 2017/6/19.
 */
public interface ConnManager {

    /**
     * 获取连接
     * @return
     * @throws SQLException
     */
    Connection getConnection() throws SQLException;

    /**
     * 执行查询，返回结果集
     * @param sql
     * @return
     * @throws SQLException
     */
    ResultSet execute(String sql) throws SQLException;

    /**
     * 执行查询，返回结果集
     * @param sql
     * @param fetchSize
     * @param args
     * @return
     * @throws SQLException
     */
    ResultSet execute(String sql, Integer fetchSize, Object... args) throws SQLException;

    /**
     * @param
     * @return
     */
    Map<String, Integer> getColumnTypes();

    /**
     * @return
     */
    String[] getColumnNames();

    /**
     * 获取主键
     * @return
     */
    String[] getPrimaryKeys();

}
