package com.deppon.hadoop.sqoopx.core.jdbc;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;

/**
 * Created by meepai on 2017/6/22.
 */
public class SqlConnManager extends JdbcConnManager {

    private static final Logger log = Logger.getLogger(SqlConnManager.class);

    public SqlConnManager(SqoopxOptions options) {
        super(options);
    }

    @Override
    protected String[] getColumnNamesByTable(String tableName) {
        String query = getColNamesQuery(tableName);
        return getColumnNamesByQuery(query);
    }

    @Override
    protected String[] getColumnNamesByQuery(String query) {
        String sqlQuery = query;
        sqlQuery = sqlQuery.replace("$CONDITIONS", "(1=0)");
        List<String> columns = new ArrayList<String>();
        ResultSet resultSet = null;
        try {
            resultSet = execute(sqlQuery);
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int cols = rsmd.getColumnCount();
            for (int i = 1; i < cols + 1; i++) {
                String colName = rsmd.getColumnLabel(i);
                if(colName == null || colName.equals("")){
                    colName = rsmd.getColumnName(i);
                    if(colName == null){
                        colName = "_RESULT_" + i;
                    }
                }
                columns.add(colName);
            }
            return columns.toArray(new String[0]);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            release();
            return null;
        } finally {
            try {
                resultSet.close();
                closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 列名直接从列类型中获取，因此列类型必须保持列名的顺序
     * @param tableName
     * @return
     */
    @Override
    protected Map<String, Integer> getColumnTypesByTable(String tableName) {
        String query = getColNamesQuery(tableName);
        return getColumnTypesByQuery(query);
    }

    @Override
    protected Map<String, Integer> getColumnTypesByQuery(String query) {
        String sqlQuery = query;
        sqlQuery = sqlQuery.replace("$CONDITIONS", "(1=0)");
        Map<String, Integer> map = new LinkedHashMap<String, Integer>();
        ResultSet resultSet = null;
        try {
            resultSet = execute(sqlQuery);
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int cols = rsmd.getColumnCount();
            for(int i=1; i<cols+1; i++){
                int typeId = rsmd.getColumnType(i);
                // 需要将无符号的int类型转换成 bigint
                if(typeId == Types.INTEGER && !rsmd.isSigned(i)){
                    typeId = Types.BIGINT;
                }
                String colName = rsmd.getColumnLabel(i);
                if(colName == null || colName.equals("")){
                    colName = rsmd.getColumnName(i);
                }
                map.put(colName, typeId);
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            release();
            return null;
        } finally {
            try {
                if(resultSet != null){
                    resultSet.close();
                }
                closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    /**
     * @param tableName
     * @return
     */
    private String getColNamesQuery(String tableName){
        return "SELECT t.* FROM " + escapeTableName(tableName) + " AS t WHERE 1=0";
    }

    /**
     * 按需要看是否需要对表名增加引号等。如 table 需要转换成 `table`
     * @param tableName
     * @return
     */
    protected String escapeTableName(String tableName){
        return tableName;
    }

    /**
     * 获取主键
     * @return
     */
    public String[] getPrimaryKeys() {
        List<String> list = new ArrayList<String>();
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = this.getConnection().getMetaData();
            rs = dbmd.getPrimaryKeys(null, null, tableName);
            if(rs != null){
                while(rs.next()){
                    list.add(rs.getString("COLUMN_NAME"));
                }
            }
        } catch (SQLException e) {
        } finally {
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
            closeConnection();
        }
        return list.toArray(new String[0]);
    }
}
