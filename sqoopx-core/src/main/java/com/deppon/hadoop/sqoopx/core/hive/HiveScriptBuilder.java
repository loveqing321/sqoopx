package com.deppon.hadoop.sqoopx.core.hive;

import com.deppon.hadoop.sqoopx.core.jdbc.ConnManager;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.util.SqlUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * 创建 hive的建表 以及加载数据的语句。
 * Created by meepai on 2017/6/26.
 */
public class HiveScriptBuilder {

    private static final Logger log = Logger.getLogger(HiveScriptBuilder.class);

    private SqoopxOptions options;

    private ConnManager connManager;

    private String inputTable;

    private String outputTable;

    private Configuration conf;

    public HiveScriptBuilder(SqoopxOptions options, ConnManager connManager, String inputTable,
                             String outputTable, Configuration conf) {
        this.options = options;
        this.connManager = connManager;
        this.inputTable = inputTable;
        this.outputTable = outputTable;
        this.conf = conf;
    }

    public String getCreateTableStmt() throws IOException {
        Properties mapping = options.getMapColumnHive();
        Map<String, Integer> columnTypes = connManager.getColumnTypes();
        String[] colNames = this.getColumnNames();
        StringBuilder sb = new StringBuilder();
        // 如果指定外部表目录 那么创建外部表
        if(StringUtils.isNotBlank(options.getHiveExternalTableDir())){
            sb.append("CREATE EXTERNAL TABLE ");
        } else {
            sb.append("CREATE TABLE ");
        }
        // 如果设置了hive表存在报错，那么无需配置 IF NOT EXISTS
        if(!options.isFailIfHiveTableExists()){
            sb.append("IF NOT EXISTS ");
        }
        if(options.getHiveDatabaseName() != null){
            sb.append("`").append(options.getHiveDatabaseName()).append("`.");
        }
        sb.append("`").append(outputTable).append("` (");
        this.checkUserMappingIfCorrect(mapping, colNames);
        boolean first = true;
        // 分区
        String partitionKey = options.getHivePartitionKey();
        for(String col : colNames){
            if(col.equals(partitionKey)){
                continue;
            }
            if(!first){
                sb.append(", ");
            }
            first = false;
            Integer type = columnTypes.get(col);
            String hiveType = mapping.getProperty(col);
            if(hiveType == null){
                hiveType = SqlUtils.toHiveType(type);
            }
            if(hiveType == null){
                throw new IOException("Hive does not support the SQL type for column " + col);
            }
            sb.append("`").append(col).append("` ").append(hiveType);
            if(SqlUtils.isHiveTypeImprovised(type)){
                log.warn("Column " + col + " had to be cast to a less precise type in Hive");
            }
        }
        sb.append(") ");
        if(partitionKey != null){
            sb.append("PARTITIONED BY (").append(partitionKey).append("STRING) ");
        }
        sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '");
        sb.append(SqlUtils.getHiveOctalCharCode(options.getFieldsTerminatedBy()));
        sb.append("' LINES TERMINATED BY '");
        sb.append(SqlUtils.getHiveOctalCharCode(options.getLinesTerminatedBy()));
        String codec = options.getCompressionCodes();
        if(codec != null && (codec.equals("lzop") || codec.equals("com.hadoop.compression.lzo.LzopCodec"))){
            sb.append("' STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'");
            sb.append(" OUTPUTFORMAT 'org.apache.hadoop.hive.sql.io.HiveIgnoreKeyTextOutputFormat'");
        } else {
            sb.append("' STORED AS TEXTFILE");
        }
        if(StringUtils.isNotBlank(options.getHiveExternalTableDir())){
            sb.append(" LOCATION '" + options.getHiveExternalTableDir() + "'");
        }
        log.debug("Create statement: " + sb.toString());
        return sb.toString();
    }

    public String getLoadDataStmt() throws IOException {
        Path finalPath = getFinalPath();
        StringBuilder sb = new StringBuilder();
        sb.append("LOAD DATA INPATH '");
        sb.append(finalPath.toString() + "'");
        if(options.isOverwriteHiveTable()){
            sb.append(" OVERWRITE");
        }
        sb.append(" INTO TABLE ");
        if(options.getHiveDatabaseName() != null){
            sb.append("`").append(options.getHiveDatabaseName()).append("`.");
        }
        sb.append("`").append(outputTable).append("`");
        if(options.getHivePartitionKey() != null){
            sb.append(" PARTITION (")
                    .append(options.getHivePartitionKey())
                    .append("=")
                    .append(options.getHivePartitionValue())
                    .append(")");
        }
        log.debug("Load statement: " + sb.toString());
        return sb.toString();
    }

    /**
     * 获取列名
     * @return
     */
    private String[] getColumnNames(){
        String[] columns = options.getColumns();
        if(columns != null){
            return columns;
        }
        return connManager.getColumnNames();
    }

    /**
     * 检查是否用户配置的mapping 在所有列中
     * @param mapping
     * @param colNames
     */
    private void checkUserMappingIfCorrect(Properties mapping, String[] colNames){
        // 检查是否用户配置的mapping 在所有列中
        for(Object column : mapping.keySet()){
            boolean found = false;
            for(String c : colNames){
                if(c.equals(column)){
                    found = true;
                    break;
                }
            }
            if(!found){
                throw new IllegalArgumentException("No column by the name " + column + " found while importing data");
            }
        }
    }

    public Path getFinalPath() throws IOException {
        String wareHouseDir = options.getWarehouseDir();
        if(wareHouseDir == null){
            wareHouseDir = "";
        } else if(!wareHouseDir.endsWith(File.separator)){
            wareHouseDir = wareHouseDir + File.separator;
        }
        String targetPath;
        String targetDir = options.getTargetDir();
        if(targetDir != null){
            targetPath = wareHouseDir + targetDir;
        } else {
            targetPath = wareHouseDir + inputTable;
        }
        Path path = new Path(targetPath);
        return path.getFileSystem(conf).makeQualified(path);
    }
}
