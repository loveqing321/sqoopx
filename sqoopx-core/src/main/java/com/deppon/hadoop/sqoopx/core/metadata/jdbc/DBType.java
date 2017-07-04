package com.deppon.hadoop.sqoopx.core.metadata.jdbc;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.util.UriUtils;

/**
 * Created by meepai on 2017/6/24.
 */
public enum DBType {

    MYSQL("jdbc:mysql:", true, "com.mysql.jdbc.Driver"),
    POSTGRES("jdbc:postgresql:", true, "org.postgresql.Driver"),
    HSQLDB("jdbc:hsqldb:", false, "org.hsqldb.jdbcDriver"),
    ORACLE("jdbc:oracle:", true, "oracle.jdbc.driver.OracleDriver"),
    SQLSERVER("jdbc:sqlserver:", false, "com.microsoft.jdbc.sqlserver.SQLServerDriver"),
    JTDS_SQLSERVER("jdbc:jtds:sqlserver:", false, "net.sourceforge.jtds.jdbc.Driver"),
    DB2("jdbc:db2:", false, "com.ibm.db2.jcc.DB2Driver"),
    NETEZZA("jdbc:netezza:", true, ""),
    CUBRID("jdbc:cubrid:", false, "");

    public String schema;

    public boolean hasDirectConnector;

    public String driverClass;

    DBType(String schema, boolean hasDirectConnector, String driverClass){
        this.schema = schema;
        this.hasDirectConnector = hasDirectConnector;
        this.driverClass = driverClass;
    }

    /**
     * @param options
     * @return
     */
    public static DBType from(SqoopxOptions options){
        String schema = UriUtils.extractSchema(options.getConnectString());
        for(DBType type : values()){
            if(schema.startsWith(type.schema)){
                return type;
            }
        }
        return null;
    }
}
