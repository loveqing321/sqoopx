package com.deppon.hadoop.sqoopx.core.jdbc;

import com.deppon.hadoop.sqoopx.core.metadata.jdbc.HiveJdbcConnManager;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;

import java.sql.SQLException;

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
    }
}
