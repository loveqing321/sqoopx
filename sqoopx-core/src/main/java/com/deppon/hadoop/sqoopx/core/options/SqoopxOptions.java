package com.deppon.hadoop.sqoopx.core.options;

import com.deppon.hadoop.sqoopx.core.conf.ConfigurationConstants;
import com.deppon.hadoop.sqoopx.core.exception.InvalidOptionsException;
import com.deppon.hadoop.sqoopx.core.file.FileLayout;
import com.deppon.hadoop.sqoopx.core.jdbc.DBType;
import com.deppon.hadoop.sqoopx.core.mapreduce.DelimiterSet;
import com.deppon.hadoop.sqoopx.core.tools.ExportTool;
import com.deppon.hadoop.sqoopx.core.tools.ImportTool;
import com.deppon.hadoop.sqoopx.core.tools.SqoopxTool;
import com.deppon.hadoop.sqoopx.core.util.IdentifierUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;

/**
 * Created by meepai on 2017/6/19.
 */
public class SqoopxOptions {

    public static final Logger log = Logger.getLogger(SqoopxOptions.class);

    /************ common arguments **************/
    /**
     * 是否调试
     */
    private boolean verbose;

    /**
     * 连接字符串
     */
    @StoredAsProperty("mapreduce.jdbc.url")
    private String connectString;

    /**
     * 连接管理器类名
     */
    private String connManagerClassName;

    /**
     * 连接参数
     */
    private Properties connectionParams;

    /**
     * 驱动名称
     */
    @StoredAsProperty("mapreduce.jdbc.driver.class")
    private String driverClassName;

    /**
     * 用户名
     */
    @StoredAsProperty("mapreduce.jdbc.username")
    private String username;

    /**
     * 密码
     */
    @StoredAsProperty("mapreduce.jdbc.password")
    private String password;

    /**
     * hadoop mapreduce home
     */
    private String hadoopMapRedHome;

    /**
     * 扩展类
     */
    private String extensionClasses;

    /**
     * 异常抛出
     */
    private boolean throwOnError;

    /**
     * 临时root目录
     */
    private String tempRootDir;

    /**
     * 临时的本地目录，用于存放临时生成的脚本等
     */
    private String tempDir;

    /**
     * 是否松懈的（读未提交）的事务隔离级别
     */
    private boolean relaxedIsolation;

    /**
     * 事务隔离级别
     */
    private int metadataTransactionIsolationLevel;

    /**
     *
     */
    private boolean oracleEscapingDisabled;

    /**
     *
     */
    private boolean escapMappingColumnNamesEnabled;

    /**
     * 密码文件路径
     */
    private String passwordFilePath;

    /**
     * 密码别名
     */
    private String passwordAlias;

    /**
     * 直接模式
     */
    private boolean directMode;

    /**
     * 分批模式
     */
    private boolean batchMode;

    /**
     * mapper任务数量
     */
    private int numMappers;

    /**
     * mapreduce任务名称
     */
    private String mapreduceJobName;

    /**
     * 导出目录
     */
    private String exportDir;

    /**
     * 已存在的jar名称
     */
    private String existingJarName;

    /**
     * 更新键列
     */
    private String updateKeyCol;

    /**
     * 分段表名
     */
    private String stagingTableName;

    /**
     * 是否清空分段表
     */
    private boolean clearStagingTable;

    /**
     * 存储过程
     */
    private String call;

    /**
     * 更新模式
     */
    private UpdateMode updateMode;

    /*************** hive arguments ***********/
    /**
     * hive home
     */
    private String hiveHome;

    /**
     * hive host地址
     */
    private String hiveHost;

    /**
     * hive port
     */
    private Integer hivePort;

    /**
     * hive username
     */
    private String hiveUsername;

    /**
     * hive 密码
     */
    private String hivePassword;

    /**
     * hive 导入标示
     */
    private boolean hiveImport;

    /**
     *
     */
    private boolean overwriteHiveTable;

    /**
     *
     */
    private String hiveDatabaseName;

    /**
     * 如果hive表存在则任务迅速失败
     */
    private boolean failIfHiveTableExists;

    /**
     *
     */
    private String hiveExternalTableDir;

    /**
     * hive表名
     */
    private String hiveTableName;

    /**
     * 是否移除分割
     */
    private boolean hiveDropDelims;

    /**
     * hive分割替换
     */
    private String hiveDelimsReplacement;

    /**
     * hive分区key
     */
    private String hivePartitionKey;

    /**
     * hive分区value
     */
    private String hivePartitionValue;

    /**
     * 列映射
     */
    private Properties mapColumnHive;

    /*************** OutputFormat arguments ***************/

    /**
     * 输出
     */
    private DelimiterSet outputDelimiterSet;

    /**
     * 明确使用分割
     */
    private boolean explicitDelims;

    /*************** InputFormat arguments ***************/

    /**
     * 输入
     */
    private DelimiterSet inputDelimiterSet;

    /**
     *
     */
    private boolean explicitInputDelims;

    /*************** CodeGen arguments ***************/
    /**
     * 源码输出目录
     */
    private String codeOutputDir;

    /**
     * jar包输出目录
     */
    private String jarOutputDir;

    /**
     * 包名
     */
    private String packageName;

    /**
     * 类名
     */
    private String className;

    /**
     * 关系数据库表名
     */
    @StoredAsProperty("mapreduce.jdbc.input.table.name")
    private String tableName;

    /**
     * 关系数据库的列
     */
    private String[] columns;

    /**
     * 查询语句，用于获取列名
     */
    private String sqlQuery;

    /**
     * 抓取数据大小
     */
    private Integer fetchSize;

    /**
     * 列与java属性的映射
     */
    private Properties mapColumnJava;

    /**
     * null字符串值
     */
    private String nullStringValue;

    /**
     *
     */
    private String inNullStringValue;

    /**
     *
     */
    private String nullNonStringValue;

    /**
     *
     */
    private String inNullNonStringValue;

    /**
     * 过滤分布式缓存
     */
    private boolean skipDistCache;

    /*************** HBase arguments ***************/

    /**
     * hbase表
     */
    private String HBaseTable;

    /**
     * 列组
     */
    private String HBaseColFamily;

    /**
     * row key
     */
    private String HBaseRowKeyColumn;

    /**
     * 是否创建表
     */
    private boolean createHBaseTable;

    /**
     *
     */
    private boolean HBaseBulkLoadEnabled;

    /**
     * HCatalog table name
     */
    private String hCatTableName;

    /**
     * HCatalog database name
     */
    private String hCatDatabaseName;

    /**
     * HCatalog storage stanza
     */
    private String hCatStorageStanza;

    /**
     *
     */
    private boolean createHCatalogTable;

    /**
     *
     */
    private boolean dropAndCreateHCatalogTable;

    /**
     *
     */
    private String hCatHome;

    /**
     *
     */
    private String hCatalogPartitionKeys;

    /**
     *
     */
    private String hCatalogPartitionValues;

    /**
     * 文件格式
     */
    private FileLayout fileLayout;

    /*************** Validation arguments ***************/

    /**
     * 是否启用验证
     */
    private boolean validationEnabled;

    /**
     * 验证器实现类
     */
    private Class validatorClass;

    /**
     * 验证开始类
     */
    private Class validationThresholdClass;

    /**
     * 验证失败处理类
     */
    private Class validationFailureHandlerClass;

    /**
     *
     */
    private String splitByCol;

    /**
     *
     */
    private int splitLimit;

    /**
     *
     */
    private String whereClause;

    /**
     *
     */
    private String targetDir;

    /**
     *
     */
    private boolean appendMode;

    /**
     *
     */
    private boolean deleteMode;

    /**
     *
     */
    private String boundaryQuery;

    /**
     *
     */
    private String mergeKeyCol;

    /**
     *
     */
    private String warehouseDir;

    /**
     *
     */
    private boolean useCompression;

    /**
     *
     */
    private String compressionCodes;

    /**
     *
     */
    private long directSplitSize;

    /**
     *
     */
    private long inlineLobLimit;

    /**
     *
     */
    private boolean autoResetToOneMapper;

    /**
     * 配置对象
     */
    private Configuration conf;

    /**
     * 所属工具类
     */
    private SqoopxTool sqoopxTool;

    /**
     * Sqoopx执行上下文
     */
    private SqoopxContext context;

    public SqoopxOptions(){
        this(null);
    }

    public SqoopxOptions(Configuration conf){
        this.init(conf);
    }

    private void init(Configuration conf){
        this.hadoopMapRedHome = System.getenv("HADOOP_MAPRED_HOME");
        String hiveHomeEnv = System.getenv("HIVE_HOME");
        this.hiveHome = System.getProperty("hive.home", hiveHomeEnv);
        this.inputDelimiterSet = new DelimiterSet(DelimiterSet.NULL_CHAR, DelimiterSet.NULL_CHAR, DelimiterSet.NULL_CHAR, DelimiterSet.NULL_CHAR, false);
        this.outputDelimiterSet = new DelimiterSet();
        this.codeOutputDir = System.getProperty("sqoopx.src.dir", ".");
        this.verbose = false;
        this.mapColumnHive = new Properties();
        this.mapColumnJava = new Properties();
    }

    /**
     * 在options所有参数初始化完之后 需要调用该方法，完成扩展参数的构建。
     * 构建其他的所需参数
     */
    public void contribute(){
        this.context = contributeContext();
        // 根据表名，构建需要输出的类名
        // 目前暂时支持这种
        if(this.tableName != null) {
            this.className = StringUtils.capitalize(IdentifierUtils.toJavaIdentifier(this.tableName));
        }
        //
        if(this.jarOutputDir == null){
            this.confirmJarOutpuDir();
        }
        // 如果没有配置驱动，需要通过url来识别驱动
        if(driverClassName == null && connectString != null){
            DBType type = DBType.from(this);
            if(type == null){
                throw new RuntimeException("Unsupported db schema: " + connectString);
            }
            driverClassName = type.driverClass;
        }
        // 将参数构建到连接字符串中 因为使用DBInputFormat无法指定连接参数。
        if(connectString != null && connectionParams != null && connectionParams.size() > 0){
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for(Map.Entry entry : connectionParams.entrySet()){
                if(!first){
                    sb.append("&");
                }
                first = false;
                sb.append(entry.getKey()).append("=").append(entry.getValue());
            }
            if(connectString.indexOf("\\?") > 0){
                connectString = connectString + "&" + sb.toString();
            } else {
                connectString = connectString + "?" + sb.toString();
            }
            connectionParams.clear();
        }
    }

    /**
     * 确定jar输出位置
     */
    private void confirmJarOutpuDir(){
        String dir = conf.get(ConfigurationConstants.JAR_OUTPUT_DIR);
        if(dir == null){
            String userDir = System.getProperty("user.dir");
            dir = System.getProperty("java.io.tmpdir", userDir);
        }
        if(dir == null){
            throw new RuntimeException("Can't make sure the jar output dir! Please check the argument --bindir.");
        }
        File file = new File(dir);
        if(!file.exists() || !file.isDirectory()){
            throw new RuntimeException("Can't make sure the jar output dir! Please check the argument --bindir.");
        }
        this.jarOutputDir = dir;
    }

    /**
     * @return
     */
    public SqoopxContext contributeContext(){
        SqoopxContext context = new SqoopxContext();
        if(this.sqoopxTool instanceof ExportTool){
            context.operation = SqoopxContext.Operation.EXPORT;
        } else if(this.sqoopxTool instanceof ImportTool){
            context.operation = SqoopxContext.Operation.IMPORT;
        }
        // 判断集群内部源
        if(this.hiveTableName != null){
            context.inner = SqoopxContext.InnerSource.HIVE;
        } else if(this.HBaseTable != null){
            context.inner = SqoopxContext.InnerSource.HBASE;
        } else {
            context.inner = SqoopxContext.InnerSource.TEXT;
        }
        // 判断集群外部源
        if(this.tableName != null){
            context.outer = SqoopxContext.OuterSource.DB;
        } else { // 目前暂时支持输出关系数据库表
            throw new RuntimeException("缺少必要的外部源配置！");
        }
        return context;
    }

    /**
     * 转换字符串为合适的字段分割符字符
     * @param charish
     * @return
     * @throws InvalidOptionsException
     */
    public static char toChar(String charish) throws InvalidOptionsException {
        if(charish != null && charish.length() > 0){
            // 如果以 "\0x" 或者 "\0X" 开头，那么认为是以16进制 数字作为字符
            if(charish.startsWith("\\0x") || charish.startsWith("\\0X")){
                if(charish.length() == 3){
                    throw new InvalidOptionsException("Base-16 value's length must great then 3!");
                }
                int val = Integer.parseInt(charish.substring(3), 16);
                return (char) val;
            } else if(charish.startsWith("\\0")){ // 如果以 "\0" 开头，那么认为是8进制数字作为字符
                if(charish.length() == 2){
                    return '\u0000';
                } else {
                    int val = Integer.parseInt(charish.substring(2), 8);
                    return (char) val;
                }
            } else if(charish.startsWith("\\")){ // 如果以 "\" 开头，那么认为是字符
                if(charish.length() == 1){
                    return '\\';
                } else if(charish.length() > 2){
                    throw new InvalidOptionsException("The length of char type field terminated cannot be great then 2!");
                } else {
                    char c = charish.charAt(1);
                    switch (c){
                        case '\"':
                            return '\"';
                        case '\'':
                            return '\'';
                        case '\\':
                            return '\\';
                        case 'b':
                            return '\b';
                        case 'n':
                            return '\n';
                        case 'r':
                            return '\r';
                        case 't':
                            return '\t';
                        default:
                            throw new InvalidOptionsException("Cannot resolve the field terminated char: " + charish);
                    }
                }
            }
        }
        throw new InvalidOptionsException("Cannot use null or blank string as field terminated char!");
    }

    /**
     * 根据Class名称获取类
     * @param className
     * @return
     * @throws InvalidOptionsException
     */
    public static Class getClassByName(String className) throws InvalidOptionsException {
        try {
            return Class.forName(className, true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new InvalidOptionsException(e.getMessage(), e);
        }
    }

    /**
     * 解析字符串到properties对象
     * @param columnMapping
     * @param target
     */
    private void parseColumnMapping(String columnMapping, Properties target){
        target.clear();
        String[] args = columnMapping.split(",");
        for(int i=0; i<args.length; i++){
            String map = args[i];
            String[] details = map.split("=");
            target.put(details[0], details[1]);
        }
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public String getConnectString() {
        return connectString;
    }

    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    public String getConnManagerClassName() {
        return connManagerClassName;
    }

    public void setConnManagerClassName(String connManagerClassName) {
        this.connManagerClassName = connManagerClassName;
    }

    public Properties getConnectionParams() {
        return connectionParams;
    }

    public void setConnectionParams(Properties connectionParams) {
        this.connectionParams = connectionParams;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setPasswordFromConsole(){
        char[] chars = System.console().readPassword("Enter password: ", new Object[0]);
        if(chars != null){
            this.password = new String(chars);
        }
    }

    public String getHadoopMapRedHome() {
        return hadoopMapRedHome;
    }

    public void setHadoopMapRedHome(String hadoopMapRedHome) {
        this.hadoopMapRedHome = hadoopMapRedHome;
    }

    public String getExtensionClasses() {
        return extensionClasses;
    }

    public void setExtensionClasses(String extensionClasses) {
        this.extensionClasses = extensionClasses;
    }

    public void setMapColumnHive(Properties mapColumnHive) {
        this.mapColumnHive = mapColumnHive;
    }

    public void setMapColumnJava(Properties mapColumnJava) {
        this.mapColumnJava = mapColumnJava;
    }

    public String getHiveHome() {
        return hiveHome;
    }

    public void setHiveHome(String hiveHome) {
        this.hiveHome = hiveHome;
    }

    public String getHiveHost() {
        return hiveHost;
    }

    public void setHiveHost(String hiveHost) {
        this.hiveHost = hiveHost;
    }

    public Integer getHivePort() {
        return hivePort;
    }

    public void setHivePort(Integer hivePort) {
        this.hivePort = hivePort;
    }

    public String getHiveUsername() {
        return hiveUsername;
    }

    public void setHiveUsername(String hiveUsername) {
        this.hiveUsername = hiveUsername;
    }

    public String getHivePassword() {
        return hivePassword;
    }

    public void setHivePassword(String hivePassword) {
        this.hivePassword = hivePassword;
    }

    public boolean isHiveImport() {
        return hiveImport;
    }

    public void setHiveImport(boolean hiveImport) {
        this.hiveImport = hiveImport;
    }

    public String getHiveDatabaseName() {
        return hiveDatabaseName;
    }

    public void setHiveDatabaseName(String hiveDatabaseName) {
        this.hiveDatabaseName = hiveDatabaseName;
    }

    public boolean isOverwriteHiveTable() {
        return overwriteHiveTable;
    }

    public void setOverwriteHiveTable(boolean overwriteHiveTable) {
        this.overwriteHiveTable = overwriteHiveTable;
    }

    public String getHiveExternalTableDir() {
        return hiveExternalTableDir;
    }

    public void setHiveExternalTableDir(String hiveExternalTableDir) {
        this.hiveExternalTableDir = hiveExternalTableDir;
    }

    public boolean isFailIfHiveTableExists() {
        return failIfHiveTableExists;
    }

    public void setFailIfHiveTableExists(boolean failIfHiveTableExists) {
        this.failIfHiveTableExists = failIfHiveTableExists;
    }

    public String getHiveTableName() {
        return hiveTableName;
    }

    public void setHiveTableName(String hiveTableName) {
        this.hiveTableName = hiveTableName;
    }

    public boolean isHiveDropDelims() {
        return hiveDropDelims;
    }

    public void setHiveDropDelims(boolean hiveDropDelims) {
        this.hiveDropDelims = hiveDropDelims;
    }

    public String getHiveDelimsReplacement() {
        return hiveDelimsReplacement;
    }

    public void setHiveDelimsReplacement(String hiveDelimsReplacement) {
        this.hiveDelimsReplacement = hiveDelimsReplacement;
    }

    public String getHivePartitionKey() {
        return hivePartitionKey;
    }

    public String getTempRootDir() {
        if(tempRootDir == null){
            return conf.get(ConfigurationConstants.TEMPORARY_ROOT_DIR);
        }
        return tempRootDir;
    }

    public void setTempRootDir(String tempRootDir) {
        this.tempRootDir = tempRootDir;
    }

    public String getTempDir() {
        if(tempDir == null){
            return conf.get(ConfigurationConstants.TEMPORARY_DIR);
        }
        return tempDir;
    }

    public void setTempDir(String tempDir) {
        this.tempDir = tempDir;
    }

    public void setHivePartitionKey(String hivePartitionKey) {
        this.hivePartitionKey = hivePartitionKey;
    }

    public String getHivePartitionValue() {
        return hivePartitionValue;
    }

    public void setHivePartitionValue(String hivePartitionValue) {
        this.hivePartitionValue = hivePartitionValue;
    }

    public Properties getMapColumnHive() {
        return mapColumnHive;
    }

    public void setMapColumnHive(String mapColumnHive) {
        this.parseColumnMapping(mapColumnHive, this.mapColumnHive);
    }

    public char getFieldsTerminatedBy() {
        return this.outputDelimiterSet.getFieldDelim();
    }

    public void setFieldsTerminatedBy(char fieldsTerminatedBy) {
        this.outputDelimiterSet.setFieldDelim(fieldsTerminatedBy);
    }

    public boolean isExplicitDelims() {
        return explicitDelims;
    }

    public void setExplicitDelims(boolean explicitDelims) {
        this.explicitDelims = explicitDelims;
    }

    public char getLinesTerminatedBy() {
        return this.outputDelimiterSet.getRecordDelim();
    }

    public void setLinesTerminatedBy(char linesTerminatedBy) {
        this.outputDelimiterSet.setRecordDelim(linesTerminatedBy);
    }

    public char getOutputEnclosedBy() {
        return this.outputDelimiterSet.getEnclosedBy();
    }

    public void setEnclosedBy(char enclosedBy) {
        this.outputDelimiterSet.setEnclosedBy(enclosedBy);
    }

    public boolean isOutputEncloseRequired() {
        return this.outputDelimiterSet.isEncloseRequired();
    }

    public void setOutputEncloseRequired(boolean outputEncloseRequired) {
        this.outputDelimiterSet.setEncloseRequired(outputEncloseRequired);
    }

    public char getOutputEscapedBy() {
        return this.outputDelimiterSet.getEscapedBy();
    }

    public void setEscapedBy(char escapedBy) {
        this.outputDelimiterSet.setEscapedBy(escapedBy);
    }

    public char getInputFieldsTerminatedBy() {
        return this.inputDelimiterSet.getFieldDelim();
    }

    public void setInputFieldsTerminatedBy(char inputFieldsTerminatedBy) {
        this.inputDelimiterSet.setFieldDelim(inputFieldsTerminatedBy);
    }

    public char getInputLinesTerminatedBy() {
        return this.inputDelimiterSet.getRecordDelim();
    }

    public void setInputLinesTerminatedBy(char inputLinesTerminatedBy) {
        this.inputDelimiterSet.setRecordDelim(inputLinesTerminatedBy);
    }

    public boolean isExplicitInputDelims() {
        return explicitInputDelims;
    }

    public void setExplicitInputDelims(boolean explicitInputDelims) {
        this.explicitInputDelims = explicitInputDelims;
    }

    public boolean isRelaxedIsolation() {
        return relaxedIsolation;
    }

    public void setRelaxedIsolation(boolean relaxedIsolation) {
        this.relaxedIsolation = relaxedIsolation;
    }

    public int getMetadataTransactionIsolationLevel() {
        return metadataTransactionIsolationLevel;
    }

    public void setMetadataTransactionIsolationLevel(int metadataTransactionIsolationLevel) {
        this.metadataTransactionIsolationLevel = metadataTransactionIsolationLevel;
    }

    public boolean isOracleEscapingDisabled() {
        return oracleEscapingDisabled;
    }

    public void setOracleEscapingDisabled(boolean oracleEscapingDisabled) {
        this.oracleEscapingDisabled = oracleEscapingDisabled;
    }

    public boolean isEscapMappingColumnNamesEnabled() {
        return escapMappingColumnNamesEnabled;
    }

    public void setEscapMappingColumnNamesEnabled(boolean escapMappingColumnNamesEnabled) {
        this.escapMappingColumnNamesEnabled = escapMappingColumnNamesEnabled;
    }

    public String getPasswordFilePath() {
        return passwordFilePath;
    }

    public void setPasswordFilePath(String passwordFilePath) {
        this.passwordFilePath = passwordFilePath;
    }

    public String getPasswordAlias() {
        return passwordAlias;
    }

    public void setPasswordAlias(String passwordAlias) {
        this.passwordAlias = passwordAlias;
    }

    public boolean isDirectMode() {
        return directMode;
    }

    public void setDirectMode(boolean directMode) {
        this.directMode = directMode;
    }

    public boolean isBatchMode() {
        return batchMode;
    }

    public void setBatchMode(boolean batchMode) {
        this.batchMode = batchMode;
    }

    public int getNumMappers() {
        return numMappers;
    }

    public void setNumMappers(int numMappers) {
        this.numMappers = numMappers;
    }

    public String getMapreduceJobName() {
        return mapreduceJobName;
    }

    public void setMapreduceJobName(String mapreduceJobName) {
        this.mapreduceJobName = mapreduceJobName;
    }

    public String getExportDir() {
        return exportDir;
    }

    public void setExportDir(String exportDir) {
        this.exportDir = exportDir;
    }

    public String getExistingJarName() {
        return existingJarName;
    }

    public void setExistingJarName(String existingJarName) {
        this.existingJarName = existingJarName;
    }

    public String getUpdateKeyCol() {
        return updateKeyCol;
    }

    public void setUpdateKeyCol(String updateKeyCol) {
        this.updateKeyCol = updateKeyCol;
    }

    public String getStagingTableName() {
        return stagingTableName;
    }

    public void setStagingTableName(String stagingTableName) {
        this.stagingTableName = stagingTableName;
    }

    public boolean isClearStagingTable() {
        return clearStagingTable;
    }

    public void setClearStagingTable(boolean clearStagingTable) {
        this.clearStagingTable = clearStagingTable;
    }

    public String getCall() {
        return call;
    }

    public void setCall(String call) {
        this.call = call;
    }

    public UpdateMode getUpdateMode() {
        return updateMode;
    }

    public void setUpdateMode(UpdateMode updateMode) {
        this.updateMode = updateMode;
    }

    public char getInputEnclosedBy() {
        return this.inputDelimiterSet.getEnclosedBy();
    }

    public void setInputEnclosedBy(char inputEnclosedBy) {
        this.inputDelimiterSet.setEnclosedBy(inputEnclosedBy);
    }

    public boolean isInputEncloseRequired() {
        return inputDelimiterSet.isEncloseRequired();
    }

    public void setInputEncloseRequired(boolean inputEncloseRequired) {
        this.inputDelimiterSet.setEncloseRequired(inputEncloseRequired);
    }

    public char getInputEscapedBy() {
        return this.inputDelimiterSet.getEscapedBy();
    }

    public void setInputEscapedBy(char inputEscapedBy) {
        this.inputDelimiterSet.setEscapedBy(inputEscapedBy);
    }

    public DelimiterSet getOutputDelimiterSet() {
        return outputDelimiterSet;
    }

    public void setOutputDelimiterSet(DelimiterSet outputDelimiterSet) {
        this.outputDelimiterSet = outputDelimiterSet;
    }

    public DelimiterSet getInputDelimiterSet() {
        return inputDelimiterSet;
    }

    public void setInputDelimiterSet(DelimiterSet inputDelimiterSet) {
        this.inputDelimiterSet = inputDelimiterSet;
    }

    public String getCodeOutputDir() {
        return codeOutputDir;
    }

    public void setCodeOutputDir(String codeOutputDir) {
        this.codeOutputDir = codeOutputDir;
    }

    public String getJarOutputDir() {
        return jarOutputDir;
    }

    public void setJarOutputDir(String jarOutputDir) {
        this.jarOutputDir = jarOutputDir;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public String getSqlQuery() {
        return sqlQuery;
    }

    public void setSqlQuery(String sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }

    public Properties getMapColumnJava() {
        return mapColumnJava;
    }

    public void setMapColumnJava(String mapColumnJava) {
        this.parseColumnMapping(mapColumnJava, this.mapColumnJava);
    }

    public String getNullStringValue() {
        return nullStringValue;
    }

    public void setNullStringValue(String nullStringValue) {
        this.nullStringValue = nullStringValue;
    }

    public String getInNullStringValue() {
        return inNullStringValue;
    }

    public void setInNullStringValue(String inNullStringValue) {
        this.inNullStringValue = inNullStringValue;
    }

    public String getNullNonStringValue() {
        return nullNonStringValue;
    }

    public void setNullNonStringValue(String nullNonStringValue) {
        this.nullNonStringValue = nullNonStringValue;
    }

    public String getInNullNonStringValue() {
        return inNullNonStringValue;
    }

    public void setInNullNonStringValue(String inNullNonStringValue) {
        this.inNullNonStringValue = inNullNonStringValue;
    }

    public boolean isSkipDistCache() {
        return skipDistCache;
    }

    public void setSkipDistCache(boolean skipDistCache) {
        this.skipDistCache = skipDistCache;
    }

    public String getHBaseTable() {
        return HBaseTable;
    }

    public void setHBaseTable(String HBaseTable) {
        this.HBaseTable = HBaseTable;
    }

    public String getHBaseColFamily() {
        return HBaseColFamily;
    }

    public void setHBaseColFamily(String HBaseColFamily) {
        this.HBaseColFamily = HBaseColFamily;
    }

    public String getHBaseRowKeyColumn() {
        return HBaseRowKeyColumn;
    }

    public void setHBaseRowKeyColumn(String HBaseRowKeyColumn) {
        this.HBaseRowKeyColumn = HBaseRowKeyColumn;
    }

    public boolean isCreateHBaseTable() {
        return createHBaseTable;
    }

    public void setCreateHBaseTable(boolean createHBaseTable) {
        this.createHBaseTable = createHBaseTable;
    }

    public boolean isHBaseBulkLoadEnabled() {
        return HBaseBulkLoadEnabled;
    }

    public void setHBaseBulkLoadEnabled(boolean HBaseBulkLoadEnabled) {
        this.HBaseBulkLoadEnabled = HBaseBulkLoadEnabled;
    }

    public String getHCatTableName() {
        return hCatTableName;
    }

    public void setHCatTableName(String hCatTableName) {
        this.hCatTableName = hCatTableName;
    }

    public String getHCatDatabaseName() {
        return hCatDatabaseName;
    }

    public void setHCatDatabaseName(String hCatDatabaseName) {
        this.hCatDatabaseName = hCatDatabaseName;
    }

    public String getHCatStorageStanza() {
        return hCatStorageStanza;
    }

    public void setHCatStorageStanza(String hCatStorageStanza) {
        this.hCatStorageStanza = hCatStorageStanza;
    }

    public boolean isCreateHCatalogTable() {
        return createHCatalogTable;
    }

    public void setCreateHCatalogTable(boolean createHCatalogTable) {
        this.createHCatalogTable = createHCatalogTable;
    }

    public boolean isDropAndCreateHCatalogTable() {
        return dropAndCreateHCatalogTable;
    }

    public void setDropAndCreateHCatalogTable(boolean dropAndCreateHCatalogTable) {
        this.dropAndCreateHCatalogTable = dropAndCreateHCatalogTable;
    }

    public String getHCatHome() {
        return hCatHome;
    }

    public void setHCatHome(String hCatHome) {
        this.hCatHome = hCatHome;
    }

    public String getHCatalogPartitionKeys() {
        return hCatalogPartitionKeys;
    }

    public void setHCatalogPartitionKeys(String hCatalogPartitionKeys) {
        this.hCatalogPartitionKeys = hCatalogPartitionKeys;
    }

    public String getHCatalogPartitionValues() {
        return hCatalogPartitionValues;
    }

    public void setHCatalogPartitionValues(String hCatalogPartitionValues) {
        this.hCatalogPartitionValues = hCatalogPartitionValues;
    }

    public FileLayout getFileLayout() {
        return fileLayout;
    }

    public void setFileLayout(FileLayout fileLayout) {
        this.fileLayout = fileLayout;
    }

    public boolean isValidationEnabled() {
        return validationEnabled;
    }

    public void setValidationEnabled(boolean validationEnabled) {
        this.validationEnabled = validationEnabled;
    }

    public Class getValidatorClass() {
        return validatorClass;
    }

    public void setValidatorClass(Class validatorClass) {
        this.validatorClass = validatorClass;
    }

    public Class getValidationThresholdClass() {
        return validationThresholdClass;
    }

    public void setValidationThresholdClass(Class validationThresholdClass) {
        this.validationThresholdClass = validationThresholdClass;
    }

    public Class getValidationFailureHandlerClass() {
        return validationFailureHandlerClass;
    }

    public void setValidationFailureHandlerClass(Class validationFailureHandlerClass) {
        this.validationFailureHandlerClass = validationFailureHandlerClass;
    }

    public boolean isThrowOnError() {
        return throwOnError;
    }

    public void setThrowOnError(boolean throwOnError) {
        this.throwOnError = throwOnError;
    }

    public String getSplitByCol() {
        return splitByCol;
    }

    public void setSplitByCol(String splitByCol) {
        this.splitByCol = splitByCol;
    }

    public int getSplitLimit() {
        return splitLimit;
    }

    public void setSplitLimit(int splitLimit) {
        this.splitLimit = splitLimit;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }

    public String getTargetDir() {
        return targetDir;
    }

    public void setTargetDir(String targetDir) {
        this.targetDir = targetDir;
    }

    public boolean isAppendMode() {
        return appendMode;
    }

    public void setAppendMode(boolean appendMode) {
        this.appendMode = appendMode;
    }

    public boolean isDeleteMode() {
        return deleteMode;
    }

    public void setDeleteMode(boolean deleteMode) {
        this.deleteMode = deleteMode;
    }

    public String getBoundaryQuery() {
        return boundaryQuery;
    }

    public void setBoundaryQuery(String boundaryQuery) {
        this.boundaryQuery = boundaryQuery;
    }

    public String getMergeKeyCol() {
        return mergeKeyCol;
    }

    public void setMergeKeyCol(String mergeKeyCol) {
        this.mergeKeyCol = mergeKeyCol;
    }

    public String getWarehouseDir() {
        return warehouseDir;
    }

    public void setWarehouseDir(String warehouseDir) {
        this.warehouseDir = warehouseDir;
    }

    public boolean getUseCompression() {
        return useCompression;
    }

    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    public String getCompressionCodes() {
        return compressionCodes;
    }

    public void setCompressionCodes(String compressionCodes) {
        this.compressionCodes = compressionCodes;
    }

    public long getDirectSplitSize() {
        return directSplitSize;
    }

    public void setDirectSplitSize(long directSplitSize) {
        this.directSplitSize = directSplitSize;
    }

    public long getInlineLobLimit() {
        return inlineLobLimit;
    }

    public void setInlineLobLimit(long inlineLobLimit) {
        this.inlineLobLimit = inlineLobLimit;
    }

    public boolean isAutoResetToOneMapper() {
        return autoResetToOneMapper;
    }

    public void setAutoResetToOneMapper(boolean autoResetToOneMapper) {
        this.autoResetToOneMapper = autoResetToOneMapper;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public SqoopxTool getSqoopxTool() {
        return sqoopxTool;
    }

    public void setSqoopxTool(SqoopxTool sqoopxTool) {
        this.sqoopxTool = sqoopxTool;
    }

    /**
     * 将所有使用注解的属性输出
     * @return
     */
    public Properties writeProperties(){
        Properties properties = new Properties();
        try {
            Field[] fields = SqoopxOptions.class.getDeclaredFields();
            for(Field field : fields){
                if(field.isAnnotationPresent(StoredAsProperty.class)){
                    StoredAsProperty annotation = field.getAnnotation(StoredAsProperty.class);
                    String key = annotation.value();
                    Class type = field.getType();
                    if(type.equals(int.class)){
                        properties.setProperty(key, Integer.toString(field.getInt(this)));
                    } else if(type.equals(boolean.class)){
                        properties.setProperty(key, Boolean.toString(field.getBoolean(this)));
                    } else if(type.equals(long.class)){
                        properties.setProperty(key, Long.toString(field.getLong(this)));
                    } else if(type.equals(String.class)){
                        properties.setProperty(key, (String) field.get(this));
                    } else if(type.equals(Integer.class)){
                        properties.setProperty(key, field.get(this) == null ? "null" : field.get(this).toString());
                    } else if(type.isEnum()){
                        properties.setProperty(key, field.get(this).toString());
                    } else if(type.equals(Map.class)){
                        // 需要转json
                    } else {
                        throw new RuntimeException("Cannot set property " + key + " for type " + type);
                    }
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return properties;
    }

    @Override
    public String toString() {
        return "SqoopxOptions{" +
                "verbose=" + verbose +
                ", connectString='" + connectString + '\'' +
                ", connManagerClassName='" + connManagerClassName + '\'' +
                ", connectionParams=" + connectionParams +
                ", driverClassName='" + driverClassName + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", hadoopMapRedHome='" + hadoopMapRedHome + '\'' +
                ", extensionClasses='" + extensionClasses + '\'' +
                ", hiveHome='" + hiveHome + '\'' +
                ", hiveImport=" + hiveImport +
                ", failIfHiveTableExists=" + failIfHiveTableExists +
                ", hiveTableName='" + hiveTableName + '\'' +
                ", hiveDropDelims=" + hiveDropDelims +
                ", hiveDelimsReplacement='" + hiveDelimsReplacement + '\'' +
                ", hivePartitionKey='" + hivePartitionKey + '\'' +
                ", hivePartitionValue='" + hivePartitionValue + '\'' +
                ", mapColumnHive=" + mapColumnHive +
                ", outputDelimiterSet=" + outputDelimiterSet +
                ", explicitDelims=" + explicitDelims +
                ", inputDelimiterSet=" + inputDelimiterSet +
                ", codeOutputDir='" + codeOutputDir + '\'' +
                ", jarOutputDir='" + jarOutputDir + '\'' +
                ", packageName='" + packageName + '\'' +
                ", className='" + className + '\'' +
                ", mapColumnJava=" + mapColumnJava +
                ", nullStringValue='" + nullStringValue + '\'' +
                ", inNullStringValue='" + inNullStringValue + '\'' +
                ", nullNonStringValue='" + nullNonStringValue + '\'' +
                ", inNullNonStringValue='" + inNullNonStringValue + '\'' +
                ", HBaseTable='" + HBaseTable + '\'' +
                ", HBaseColFamily='" + HBaseColFamily + '\'' +
                ", HBaseRowKeyColumn='" + HBaseRowKeyColumn + '\'' +
                ", createHBaseTable=" + createHBaseTable +
                ", validationEnabled=" + validationEnabled +
                ", validatorClass=" + validatorClass +
                ", validationThresholdClass=" + validationThresholdClass +
                ", validationFailureHandlerClass=" + validationFailureHandlerClass +
                ", conf=" + conf +
                ", sqoopxTool=" + sqoopxTool +
                '}';
    }

    public enum UpdateMode {
        // 只更新
        UpdateOnly,
        // 允许插入
        AllowInsert;
    }

    public static final String DEF_HIVE_HOME = "/usr/lib/hive";
    public static final String DEF_HCAT_HOME = "/usr/lib/hive-hcatalog";
    public static final String DEF_HCAT_HOME_OLD = "/usr/lib/hcatalog";

    /**
     * @return
     */
    public static String getHCatHomeDefault(){
        String hCatHome = System.getenv("HCAT_HOME");
        hCatHome = System.getProperty("hcat.home", hCatHome);
        if(hCatHome == null){
            File file = new File(DEF_HCAT_HOME);
            if(file.exists()){
                hCatHome = DEF_HCAT_HOME;
            } else {
                hCatHome = DEF_HCAT_HOME_OLD;
            }
        }
        return hCatHome;
    }
}
