package com.deppon.hadoop.sqoopx.core.jdbc;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by meepai on 2017/6/29.
 */
public class HiveJdbcConnManagerTest {

    public static void main(String[] args) throws SQLException {

        SqoopxOptions options = new SqoopxOptions();
        options.setHiveHost("192.168.10.229");
        options.setHivePort(10000);
        options.setHiveDatabaseName("default");
        options.setHiveUsername("root");
        options.setHivePassword("root123");
        HiveJdbcConnManager manager = new HiveJdbcConnManager(options);
        manager.execute("select * from a");
        manager.close();

//        try {
//            Class.forName("org.apache.hive.jdbc.HiveDriver");
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
////        Properties newProps = new Properties();
////        if(this.username != null) {
////            newProps.put("user", this.username);
////            newProps.put("password", this.password);
////        }
//        Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "");
//        Statement stmt = conn.createStatement();
//        stmt.execute("select * from test_word");

    }
}
