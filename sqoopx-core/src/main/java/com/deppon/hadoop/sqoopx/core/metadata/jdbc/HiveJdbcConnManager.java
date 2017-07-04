package com.deppon.hadoop.sqoopx.core.metadata.jdbc;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Created by meepai on 2017/6/29.
 */
public class HiveJdbcConnManager {

    private static final Logger log = Logger.getLogger(HiveJdbcConnManager.class);

    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    private String url;

    private String username;

    private String password;

    private Statement lastStatement;

    private Connection lastConnection;

    public HiveJdbcConnManager(SqoopxOptions options){
        String hiveHost = options.getHiveHost();
        Integer port = options.getHivePort();
        String database = options.getHiveDatabaseName() == null ? "default" : options.getHiveDatabaseName();
        this.url = "jdbc:hive2://" + hiveHost + ":" + port + "/" + database;
        this.username = options.getHiveUsername();
        this.password = options.getHivePassword();
    }

    public Connection getConnection() throws SQLException {
        try {
            Class.forName(HIVE_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        this.lastConnection = DriverManager.getConnection(this.url, this.username == null ? "" : this.username, this.password == null ? "" : this.password);
        return this.lastConnection;
    }

    /**
     * @param sqls
     * @throws SQLException
     */
    public void executeBatch(String... sqls) throws SQLException {
        this.lastStatement = this.getConnection().createStatement();
        this.lastConnection.setAutoCommit(false);
        for(int i=0; i<sqls.length; i++){
            System.out.println(sqls[i]);
            this.lastStatement.execute(sqls[i]);
        }
        this.lastConnection.commit();
    }

    /**
     * 执行
     * @param sql
     */
    public void execute(String sql) throws SQLException {
        this.lastStatement = this.getConnection().createStatement();
        this.lastStatement.executeUpdate(sql);
    }

    /**
     * @return
     */
    public ResultSet executeQuery(String sql, Integer fetchSize, Object... args) throws SQLException {
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
        ResultSet rs =  psmt.executeQuery();
        return rs;
    }

    public void close(){
        try {
            if(this.lastStatement != null){
                this.lastStatement.close();
            }
            if(this.lastConnection != null){
                this.lastConnection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
