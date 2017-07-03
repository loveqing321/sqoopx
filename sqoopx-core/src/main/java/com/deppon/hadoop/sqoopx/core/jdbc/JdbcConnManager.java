package com.deppon.hadoop.sqoopx.core.jdbc;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.google.common.base.Preconditions;

import java.sql.*;
import java.util.Map;
import java.util.Properties;

/**
 * Created by meepai on 2017/6/22.
 */
public abstract class JdbcConnManager implements ConnManager {

    protected Statement lastStatement;

    protected SqoopxOptions options;

    protected String url;

    protected String username;

    protected String password;

    protected Properties params;

    protected String tableName;

    protected String sqlQuery;

    protected Connection lastConnection;

    public JdbcConnManager(SqoopxOptions options){
        Preconditions.checkArgument(options.getConnectString() != null);
        this.options = options;
        this.init();
    }

    private void init(){
        this.url = options.getConnectString();
        this.username = options.getUsername();
        this.password = options.getPassword();
        this.params = options.getConnectionParams();
        this.tableName = options.getTableName();
        this.sqlQuery = options.getSqlQuery();
    }

    /**
     * 获取连接
     * @return
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {
        Properties newProps = new Properties();
        if(this.params != null){
            newProps.putAll(this.params);
        }
        if(this.username != null) {
            newProps.put("user", this.username);
            newProps.put("password", this.password);
        }
        this.lastConnection = DriverManager.getConnection(this.url, newProps);
        return lastConnection;
    }

    /**
     * @param sql
     * @return
     * @throws SQLException
     */
    public ResultSet execute(String sql) throws SQLException {
        return execute(sql, this.options.getFetchSize() == null ? Integer.MIN_VALUE : this.options.getFetchSize());
    }

    /**
     * @param sql
     * @param fetchSize
     * @param args
     * @return
     * @throws SQLException
     */
    public ResultSet execute(String sql, Integer fetchSize, Object... args) throws SQLException {
        // 释放上次查询
        release();
        PreparedStatement psmt = null;
        psmt = this.getConnection().prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if(fetchSize != null){
            psmt.setFetchSize(fetchSize);
        }
        this.lastStatement = psmt;
        if(args != null){
            for(int i=0; i<args.length; i++){
                psmt.setObject(i + 1, args[i]);
            }
        }
        return psmt.executeQuery();
    }

    /**
     * 关闭上次查询
     */
    protected void release(){
        if(lastStatement != null){
            try {
                lastStatement.cancel();
            } catch (SQLException e) {
            } finally {
                lastStatement = null;
            }
        }
    }

    /**
     * 关闭连接
     */
    protected void closeConnection(){
        if(lastConnection != null){
            try {
                lastConnection.close();
            } catch (SQLException e) {
            } finally {
                lastConnection = null;
            }
        }
    }

    /**
     * 获取列类型
     * @return
     */
    public Map<String, Integer> getColumnTypes() {
        if(this.tableName != null){
            return getColumnTypesByTable(this.tableName);
        } else if(this.sqlQuery != null){
            return getColumnTypesByQuery(this.sqlQuery);
        }
        return null;
    }

    /**
     * 获取列名
     * @return
     */
    public String[] getColumnNames() {
        if(this.tableName != null){
            return getColumnNamesByTable(this.tableName);
        } else if(this.sqlQuery != null){
            return getColumnNamesByQuery(this.sqlQuery);
        }
        return null;
    }

    protected abstract String[] getColumnNamesByTable(String tableName);

    protected abstract String[] getColumnNamesByQuery(String query);

    protected abstract Map<String, Integer> getColumnTypesByTable(String tableName);

    protected abstract Map<String, Integer> getColumnTypesByQuery(String query);
}
