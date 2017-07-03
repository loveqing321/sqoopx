package com.deppon.hadoop.sqoopx.core.options;

import com.deppon.hadoop.sqoopx.core.cli.OptionFactory;
import com.deppon.hadoop.sqoopx.core.cli.RelatedOptions;
import com.deppon.hadoop.sqoopx.core.cli.SqoopxParser;
import com.deppon.hadoop.sqoopx.core.cli.ToolOptions;
import com.deppon.hadoop.sqoopx.core.exception.InvalidOptionsException;
import com.deppon.hadoop.sqoopx.core.file.FileLayout;
import com.deppon.hadoop.sqoopx.core.jdbc.JdbcTransactionLevels;
import com.deppon.hadoop.sqoopx.core.mapreduce.DelimiterSet;
import com.deppon.hadoop.sqoopx.core.tools.SqoopxTool;
import com.deppon.hadoop.sqoopx.core.util.CredentialsUtil;
import com.deppon.hadoop.sqoopx.core.util.HCatalogUtils;
import com.deppon.hadoop.sqoopx.core.util.LoggingUtils;
import com.deppon.hadoop.sqoopx.core.util.password.CredentialProviderHelper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;

/**
 * 基础的选项配置器，封装了选项配置的主流程，其子类实现具体的参数项配置即可。
 * Created by meepai on 2017/6/19.
 */
public abstract class BaseOptionsConfigurer implements OptionsConfigurer {

    private static final Logger log = Logger.getLogger(BaseOptionsConfigurer.class);

    private SqoopxTool sqoopxTool;

    private String[] extraArguments;

    /**
     * 解析参数并返回SqoopxOptions对象
     * @param args
     * @param configuration
     * @param options
     * @param useGenericOptions
     * @return
     * @throws ParseException
     * @throws InvalidOptionsException
     */
    public SqoopxOptions parseArguments(String[] args, Configuration configuration, SqoopxOptions options, boolean useGenericOptions) throws ParseException, InvalidOptionsException {
        SqoopxOptions out = options;
        if(options == null){
            out = new SqoopxOptions();
        }
        if(configuration != null){
            out.setConf(configuration);
        } else {
            out.setConf(new Configuration());
        }
        out.setSqoopxTool(sqoopxTool);
        String[] toolArgs = args;
        if(useGenericOptions){
            try {
                GenericOptionsParser genericParser = new GenericOptionsParser(out.getConf(), toolArgs);
                toolArgs = genericParser.getRemainingArgs();
            } catch (IOException e) {
                ParseException ex = new ParseException("cannot parse the args using generic parser: " + Arrays.toString(toolArgs));
                throw ex;
            }
        }
        // 构建cli Options -> CommandLine -> fill SqoopxOptions
        ToolOptions toolOptions = new ToolOptions();
        this.configureOptions(toolOptions);
        SqoopxParser parser = new SqoopxParser();
        CommandLine commandLine = parser.parse(toolOptions.merge(), toolArgs, true);
        this.applyOptions(commandLine, out);
        this.extraArguments = commandLine.getArgs();

        return out;
    }

    public String[] getExtraArguments() {
        return extraArguments;
    }

    /**
     * Common 选项
     * @return
     */
    protected RelatedOptions getCommonOptions(){
        RelatedOptions commonOptions = new RelatedOptions("Common arguments");

        commonOptions.addOption(OptionFactory.OPT_CONNECT);
        commonOptions.addOption(OptionFactory.OPT_CONN_MANAGER);
        commonOptions.addOption(OptionFactory.OPT_CONN_PARAM_FILE);
        commonOptions.addOption(OptionFactory.OPT_DRIVER);
        commonOptions.addOption(OptionFactory.OPT_USERNAME);
        commonOptions.addOption(OptionFactory.OPT_PASSWORD);
        commonOptions.addOption(OptionFactory.OPT_PASSWORD_FILE);
        commonOptions.addOption(OptionFactory.OPT_P);
        commonOptions.addOption(OptionFactory.OPT_PASSWORD_ALIAS);
        commonOptions.addOption(OptionFactory.OPT_HADOOP_MAPRED_HOME);
        commonOptions.addOption(OptionFactory.OPT_HADOOP_HOME);
        commonOptions.addOption(OptionFactory.OPT_SKIP_DIST_CACHE);
        commonOptions.addOption(OptionFactory.OPT_VERBOSE);
        commonOptions.addOption(OptionFactory.OPT_HELP);
        commonOptions.addOption(OptionFactory.OPT_TEMPORARY_ROOTDIR);
        commonOptions.addOption(OptionFactory.OPT_EXTENSION_CLASSES);
        commonOptions.addOption(OptionFactory.OPT_METADATA_TRANSACTION_ISOLATION_LEVEL);
        commonOptions.addOption(OptionFactory.OPT_THROW_ON_ERROR);
        commonOptions.addOption(OptionFactory.OPT_RELAXED_ISOLATION);
        commonOptions.addOption(OptionFactory.OPT_ORACLE_ESCAPING_DISABLED);

        return commonOptions;
    }

    /**
     * Common
     * @param commandLine
     * @param options
     * @throws InvalidOptionsException
     */
    public void applyCommonOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException {
        if(commandLine.hasOption(OptionFactory.CLI_VERBOSE)){
            options.setVerbose(true);
            LoggingUtils.setDebugLevel();
        }
        if(commandLine.hasOption(OptionFactory.CLI_HELP)){
            ToolOptions toolOptions = new ToolOptions();
            configureOptions(toolOptions);
            toolOptions.printHelp();
            throw new InvalidOptionsException("");
        }
        if(commandLine.hasOption(OptionFactory.CLI_TEMPORARY_ROOTDIR)){
            options.setTempRootDir(commandLine.getOptionValue(OptionFactory.CLI_TEMPORARY_ROOTDIR));
        }
        if(commandLine.hasOption(OptionFactory.CLI_THROW_ON_ERROR)){
            options.setThrowOnError(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_CONNECT)){
            options.setConnectString(commandLine.getOptionValue(OptionFactory.CLI_CONNECT));
        }
        if(commandLine.hasOption(OptionFactory.CLI_CONN_MANAGER)){
            options.setConnManagerClassName(commandLine.getOptionValue(OptionFactory.CLI_CONN_MANAGER));
        }
        if(commandLine.hasOption(OptionFactory.CLI_CONN_PARAM_FILE)){
            File paramFile = new File(commandLine.getOptionValue(OptionFactory.CLI_CONN_PARAM_FILE));
            if(!paramFile.exists() || !paramFile.isFile()){
                throw new InvalidOptionsException("Specified connection parameter file not found: " + paramFile);
            }
            InputStream in = null;
            Properties props = new Properties();
            try {
                in = new FileInputStream(paramFile);
                props.load(in);
            } catch (FileNotFoundException e) {
                throw new InvalidOptionsException("Specified connection parameter file not found: " + paramFile);
            } catch (IOException e) {
                throw new InvalidOptionsException("Specified connection parameter file not found: " + paramFile);
            } finally {
                IOUtils.closeQuietly(in);
            }
            options.setConnectionParams(props);
        }
        if(commandLine.hasOption(OptionFactory.CLI_NULL_STRING)){
            options.setNullStringValue(commandLine.getOptionValue(OptionFactory.CLI_NULL_STRING));
        }
        if(commandLine.hasOption(OptionFactory.CLI_INPUT_NULL_STRING)){
            options.setInNullStringValue(commandLine.getOptionValue(OptionFactory.CLI_INPUT_NULL_STRING));
        }
        if(commandLine.hasOption(OptionFactory.CLI_NULL_NON_STRING)){
            options.setNullNonStringValue(commandLine.getOptionValue(OptionFactory.CLI_NULL_NON_STRING));
        }
        if(commandLine.hasOption(OptionFactory.CLI_INPUT_NULL_NON_STRING)){
            options.setInNullNonStringValue(commandLine.getOptionValue(OptionFactory.CLI_INPUT_NULL_NON_STRING));
        }
        if(commandLine.hasOption(OptionFactory.CLI_DRIVER)){
            options.setDriverClassName(commandLine.getOptionValue(OptionFactory.CLI_DRIVER));
        }
        if(commandLine.hasOption(OptionFactory.CLI_SKIP_DIST_CACHE)){
            options.setSkipDistCache(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_HADOOP_MAPRED_HOME)){
            options.setHadoopMapRedHome(commandLine.getOptionValue(OptionFactory.CLI_HADOOP_MAPRED_HOME));
        } else if(commandLine.hasOption(OptionFactory.CLI_HADOOP_HOME)){
            options.setHadoopMapRedHome(commandLine.getOptionValue(OptionFactory.CLI_HADOOP_HOME));
        }
        if(commandLine.hasOption(OptionFactory.CLI_RELAXED_ISOLATION)){
            options.setRelaxedIsolation(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_METADATA_TRANSACTION_ISOLATION_LEVEL)){
            String transactionLevel = commandLine.getOptionValue(OptionFactory.CLI_METADATA_TRANSACTION_ISOLATION_LEVEL);
            try {
                options.setMetadataTransactionIsolationLevel(JdbcTransactionLevels.valueOf(transactionLevel).value);
            } catch (IllegalArgumentException e){
                throw new RuntimeException("Only support 0 - 3 four transaction isolation!");
            }
        }
        if(commandLine.hasOption(OptionFactory.CLI_ORACLE_ESCAPING_DISABLED)){
            options.setOracleEscapingDisabled(Boolean.parseBoolean(commandLine.getOptionValue(OptionFactory.CLI_ORACLE_ESCAPING_DISABLED)));
        }
        if(commandLine.hasOption(OptionFactory.CLI_ESCAPE_MAPPING_COLUMN_NAMES)){
            options.setEscapMappingColumnNamesEnabled(Boolean.parseBoolean(commandLine.getOptionValue(OptionFactory.CLI_ESCAPE_MAPPING_COLUMN_NAMES)));
        }
        // 认证相关
        this.applyCredentialsOptions(commandLine, options);
    }

    /**
     * 验证通用选项 必须要指定连接字符串
     * @param options
     * @throws InvalidOptionsException
     */
    protected void validateCommonOptions(SqoopxOptions options) throws InvalidOptionsException {
        if(options.getConnectString() == null){
            throw new InvalidOptionsException("Error: required argument --connect is missing");
        }
    }

    /**
     * 认证相关选项
     * @param commandLine
     * @param options
     */
    protected void applyCredentialsOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException {
        if(commandLine.hasOption(OptionFactory.CLI_USERNAME)){
            options.setUsername(commandLine.getOptionValue(OptionFactory.CLI_USERNAME));
        }
        if(commandLine.hasOption(OptionFactory.CLI_PASSWORD)){
            options.setPassword(commandLine.getOptionValue(OptionFactory.CLI_PASSWORD));
        }
        if(commandLine.hasOption(OptionFactory.CLI_P)){
            try {
                String password = new String(System.console().readPassword("Enter password:"));
                options.setPassword(password);
            } catch (NullPointerException e){
                options.setPassword(null);
            }
        }
        if(commandLine.hasOption(OptionFactory.CLI_PASSWORD_FILE)){
            if(commandLine.hasOption(OptionFactory.CLI_PASSWORD) ||
                    commandLine.hasOption(OptionFactory.CLI_P) ||
                    commandLine.hasOption(OptionFactory.CLI_PASSWORD_ALIAS)){
                throw new InvalidOptionsException("Only one of password, password alias or path to a password file must be specified.");
            }
            options.setPasswordFilePath(commandLine.getOptionValue(OptionFactory.CLI_PASSWORD_FILE));
            try {
                options.setPassword(CredentialsUtil.fetchPassword(options));
            } catch (IOException e) {
                throw new InvalidOptionsException(e);
            }
        }
        if(commandLine.hasOption(OptionFactory.CLI_PASSWORD_ALIAS)){
            if(commandLine.hasOption(OptionFactory.CLI_PASSWORD) ||
                    commandLine.hasOption(OptionFactory.CLI_P) ||
                    commandLine.hasOption(OptionFactory.CLI_PASSWORD_FILE)){
                throw new InvalidOptionsException("Only one of password, password alias or path to a password file must be specified.");
            }
            options.setPasswordAlias(commandLine.getOptionValue(OptionFactory.CLI_PASSWORD_ALIAS));
            try {
                options.setPassword(CredentialProviderHelper.resolveAlias(options.getConf(), options.getPasswordAlias()));
            } catch (IOException e) {
                throw new InvalidOptionsException(e);
            }
        }
    }

    /**
     * 验证选项
     * @return
     */
    protected RelatedOptions getValidationOption(){
        // validation 部分
        RelatedOptions validationOptions = new RelatedOptions("Validation arguments");

        validationOptions.addOption(OptionFactory.OPT_VALIDATE);
        validationOptions.addOption(OptionFactory.OPT_VALIDATOR);
        validationOptions.addOption(OptionFactory.OPT_VALIDATION_THRESHOLD);
        validationOptions.addOption(OptionFactory.OPT_VALIDATION_FAILUREHANDLER);

        return validationOptions;
    }

    /**
     * @param commandLine
     * @param options
     * @throws InvalidOptionsException
     */
    protected void applyValidationOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException {
        if(commandLine.hasOption(OptionFactory.CLI_VALIDATE)){
            options.setValidationEnabled(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_VALIDATOR)){
            options.setValidatorClass(getClassByName(commandLine.getOptionValue(OptionFactory.CLI_VALIDATOR)));
        }
        if(commandLine.hasOption(OptionFactory.CLI_VALIDATION_THRESHOLD)){
            options.setValidationThresholdClass(getClassByName(commandLine.getOptionValue(OptionFactory.CLI_VALIDATION_THRESHOLD)));
        }
        if(commandLine.hasOption(OptionFactory.CLI_VALIDATION_FAILUREHANDLER)){
            options.setValidationFailureHandlerClass(getClassByName(commandLine.getOptionValue(OptionFactory.CLI_VALIDATION_FAILUREHANDLER)));
        }
    }

    /**
     * 输入选项
     * @return
     */
    protected RelatedOptions getInputFormatOptions(){
        RelatedOptions inputFormatOptions = new RelatedOptions("Input parsing arguments");

        inputFormatOptions.addOption(OptionFactory.OPT_INPUT_FIELDS_TERMINATED_BY);
        inputFormatOptions.addOption(OptionFactory.OPT_INPUT_LINES_TERMINATED_BY);
        inputFormatOptions.addOption(OptionFactory.OPT_INPUT_OPTIONALLY_ENCLOSED_BY);
        inputFormatOptions.addOption(OptionFactory.OPT_INPUT_ENCLOSED_BY);
        inputFormatOptions.addOption(OptionFactory.OPT_INPUT_ESCAPED_BY);

        return inputFormatOptions;
    }

    /**
     * @param commandLine
     * @param options
     * @throws InvalidOptionsException
     */
    protected void applyInputFormatOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException {
        if(commandLine.hasOption(OptionFactory.CLI_INPUT_FIELDS_TERMINATED_BY)){
            options.setInputFieldsTerminatedBy(SqoopxOptions.toChar(commandLine.getOptionValue(OptionFactory.CLI_INPUT_FIELDS_TERMINATED_BY)));
            options.setExplicitInputDelims(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_INPUT_LINES_TERMINATED_BY)){
            options.setInputLinesTerminatedBy(SqoopxOptions.toChar(commandLine.getOptionValue(OptionFactory.CLI_INPUT_LINES_TERMINATED_BY)));
            options.setExplicitInputDelims(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_INPUT_OPTIONALLY_ENCLOSED_BY)){
            options.setInputEnclosedBy(SqoopxOptions.toChar(commandLine.getOptionValue(OptionFactory.CLI_INPUT_OPTIONALLY_ENCLOSED_BY)));
            options.setInputEncloseRequired(false);
            options.setExplicitInputDelims(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_INPUT_ENCLOSED_BY)){
            options.setInputEnclosedBy(SqoopxOptions.toChar(commandLine.getOptionValue(OptionFactory.CLI_INPUT_ENCLOSED_BY)));
            options.setInputEncloseRequired(true);
            options.setExplicitInputDelims(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_ESCAPED_BY)){
            options.setInputEnclosedBy(SqoopxOptions.toChar(commandLine.getOptionValue(OptionFactory.CLI_ESCAPED_BY)));
            options.setExplicitInputDelims(true);
        }
    }

    /**
     * 获取输出相关参数
     * @return
     */
    protected RelatedOptions getOutputFormatOptions(){
        RelatedOptions options = new RelatedOptions("Output line formatting arguments");
        options.addOption(OptionFactory.OPT_FIELDS_TERMINATED_BY);
        options.addOption(OptionFactory.OPT_LINES_TERMINATED_BY);
        options.addOption(OptionFactory.OPT_OPTIONALLY_ENCLOSED_BY);
        options.addOption(OptionFactory.OPT_ENCLOSED_BY);
        options.addOption(OptionFactory.OPT_ESCAPED_BY);
        options.addOption(OptionFactory.OPT_MYSQL_DELIMITERS);
        return options;
    }

    /**
     * @param commandLine
     * @param options
     * @throws InvalidOptionsException
     */
    protected void applyOutputFormatOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException {
        if(commandLine.hasOption(OptionFactory.CLI_FIELDS_TERMINATED_BY)){
            options.setFieldsTerminatedBy(SqoopxOptions.toChar(commandLine.getOptionValue(OptionFactory.CLI_FIELDS_TERMINATED_BY)));
            options.setExplicitDelims(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_LINES_TERMINATED_BY)){
            options.setLinesTerminatedBy(SqoopxOptions.toChar(commandLine.getOptionValue(OptionFactory.CLI_LINES_TERMINATED_BY)));
            options.setExplicitDelims(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_OPTIONALLY_ENCLOSED_BY)){
            options.setEnclosedBy(SqoopxOptions.toChar(commandLine.getOptionValue(OptionFactory.CLI_OPTIONALLY_ENCLOSED_BY)));
            options.setOutputEncloseRequired(false);
            options.setExplicitDelims(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_ENCLOSED_BY)){
            options.setEnclosedBy(SqoopxOptions.toChar(commandLine.getOptionValue(OptionFactory.CLI_ENCLOSED_BY)));
            options.setExplicitDelims(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_ESCAPED_BY)){
            options.setEscapedBy(SqoopxOptions.toChar(commandLine.getOptionValue(OptionFactory.CLI_ESCAPED_BY)));
            options.setExplicitDelims(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_MYSQL_DELIMITERS)){
            options.setOutputEncloseRequired(false);
            options.setFieldsTerminatedBy(',');
            options.setLinesTerminatedBy('\n');
            options.setEscapedBy('\\');
            options.setEnclosedBy('\'');
            options.setExplicitDelims(true);
        }
    }

    /**
     * 验证输出格式，主要晚上hive 导入功能的分割符设置
     * @param options
     * @throws InvalidOptionsException
     */
    protected void validateOutputFormatOptions(SqoopxOptions options) throws InvalidOptionsException {
        // 完善hive 导入参数
        if(options.isHiveImport()){
            // 如果没有指定确切的分隔符，则设置默认的hive的分隔符
            if(!options.isExplicitDelims()){
                options.setOutputDelimiterSet(DelimiterSet.HIVE_DELIMITERS);
            }
            if(options.getOutputEscapedBy() != DelimiterSet.NULL_CHAR){
                log.warn("Hive does not support escaped characters in fields;");
                log.warn("parse errors in Hive may result from using --escaped-by.");
            }
            if(options.getOutputEnclosedBy() != DelimiterSet.NULL_CHAR){
                log.warn("Hive does not support quoted strings; parse errors");
                log.warn("in Hive may result from using --enclosed-by.");
            }
        }
    }

    /**
     * CodeGen 选项
     * @return
     */
    protected RelatedOptions getCodeGenOptions(boolean multiTable){
        RelatedOptions codeGenOptions = new RelatedOptions("Code generation arguments");

        codeGenOptions.addOption(OptionFactory.OPT_OUT_DIR);
        codeGenOptions.addOption(OptionFactory.OPT_BIN_DIR);
        codeGenOptions.addOption(OptionFactory.OPT_PACKAGE_NAME);
        codeGenOptions.addOption(OptionFactory.OPT_NULL_STRING);
        codeGenOptions.addOption(OptionFactory.OPT_INPUT_NULL_STRING);
        codeGenOptions.addOption(OptionFactory.OPT_NULL_NON_STRING);
        codeGenOptions.addOption(OptionFactory.OPT_INPUT_NULL_NON_STRING);
        codeGenOptions.addOption(OptionFactory.OPT_MAP_COLUMN_JAVA);
        codeGenOptions.addOption(OptionFactory.OPT_ESCAPE_MAPPING_COLUMN_NAMES);
        codeGenOptions.addOption(OptionFactory.OPT_JAR_FILE);
        if(!multiTable){
            // 当指定class-name 后将覆盖 package-name 参数。当与 jar-file参数结合使用时，class-name用于指定jar中的input class。
            codeGenOptions.addOption(OptionFactory.OPT_CLASS_NAME);
        }

        return codeGenOptions;
    }

    /**
     * @param commandLine
     * @param options
     * @throws InvalidOptionsException
     */
    protected void applyCodeGenOptions(CommandLine commandLine, SqoopxOptions options, boolean multiTable) throws InvalidOptionsException {
        if(commandLine.hasOption(OptionFactory.CLI_OUT_DIR)){
            options.setCodeOutputDir(commandLine.getOptionValue(OptionFactory.CLI_OUT_DIR));
        }
        if(commandLine.hasOption(OptionFactory.CLI_BIN_DIR)){
            options.setJarOutputDir(commandLine.getOptionValue(OptionFactory.CLI_BIN_DIR));
        }
        if(commandLine.hasOption(OptionFactory.CLI_PACKAGE_NAME)){
            options.setPackageName(commandLine.getOptionValue(OptionFactory.CLI_PACKAGE_NAME));
        }
        if(commandLine.hasOption(OptionFactory.CLI_MAP_COLUMN_JAVA)){
            options.setMapColumnJava(commandLine.getOptionValue(OptionFactory.CLI_MAP_COLUMN_JAVA));
        }
        if(!multiTable && commandLine.hasOption(OptionFactory.CLI_CLASS_NAME)){
            options.setClassName(commandLine.getOptionValue(OptionFactory.CLI_CLASS_NAME));
        }
        if(commandLine.hasOption(OptionFactory.CLI_ESCAPE_MAPPING_COLUMN_NAMES)){
            options.setEscapMappingColumnNamesEnabled(Boolean.parseBoolean(commandLine.getOptionValue(OptionFactory.CLI_ESCAPE_MAPPING_COLUMN_NAMES)));
        }
    }

    /**
     * 验证Codegen
     * @param options
     * @throws InvalidOptionsException
     */
    protected void validateCodeGenOptions(SqoopxOptions options) throws InvalidOptionsException {
        if(options.getClassName() != null && options.getPackageName() != null){
            throw new InvalidOptionsException("--class-name overrides --package-name. You cannot use both.");
        }
    }

    /**
     * HCatalog 参数
     * @return
     */
    protected RelatedOptions getHCatalogOptions(){
        RelatedOptions hCatalogOptions = new RelatedOptions("HCatalog arguments");

        hCatalogOptions.addOption(OptionFactory.OPT_HCATALOG_TABLE);
        hCatalogOptions.addOption(OptionFactory.OPT_HCATALOG_DATABASE);
        hCatalogOptions.addOption(OptionFactory.OPT_HIVE_HOME);
        hCatalogOptions.addOption(OptionFactory.OPT_HCATALOG_HOME);
        hCatalogOptions.addOption(OptionFactory.OPT_HIVE_PARTITION_KEY);
        hCatalogOptions.addOption(OptionFactory.OPT_HIVE_PARTITION_VALUE);
        hCatalogOptions.addOption(OptionFactory.OPT_MAP_COLUMN_HIVE);
        hCatalogOptions.addOption(OptionFactory.OPT_HCATALOG_PARTITION_KEYS);
        hCatalogOptions.addOption(OptionFactory.OPT_HCATALOG_PARTITION_VALUES);

        return hCatalogOptions;
    }

    /**
     * @param commandLine
     * @param options
     * @throws InvalidOptionsException
     */
    protected void applyHCatalogOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException {
        if(commandLine.hasOption(OptionFactory.CLI_HCATALOG_TABLE)){
            options.setHCatTableName(commandLine.getOptionValue(OptionFactory.CLI_HCATALOG_TABLE));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HCATALOG_DATABASE)){
            options.setHCatDatabaseName(commandLine.getOptionValue(OptionFactory.CLI_HCATALOG_DATABASE));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HCATALOG_STORAGE_STANZA)){
            options.setHCatStorageStanza(commandLine.getOptionValue(OptionFactory.CLI_HCATALOG_STORAGE_STANZA));
        }
        if(commandLine.hasOption(OptionFactory.CLI_CREATE_HCATALOG_TABLE)){
            options.setCreateHCatalogTable(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_DROP_AND_CREATE_HCATALOG_TABLE)){
            options.setDropAndCreateHCatalogTable(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_HCATALOG_HOME)){
            options.setHCatHome(commandLine.getOptionValue(OptionFactory.CLI_HCATALOG_HOME));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HCATALOG_PARTITION_KEYS)){
            options.setHCatalogPartitionKeys(commandLine.getOptionValue(OptionFactory.CLI_HCATALOG_PARTITION_KEYS));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HCATALOG_PARTITION_VALUES)){
            options.setHCatalogPartitionValues(commandLine.getOptionValue(OptionFactory.CLI_HCATALOG_PARTITION_VALUES));
        }
        // 支持一些hive选项
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_HOME)){
            options.setHiveHome(commandLine.getOptionValue(OptionFactory.CLI_HIVE_HOME));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_HOST)){
            options.setHiveHost(commandLine.getOptionValue(OptionFactory.CLI_HIVE_HOST));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_PORT)){
            options.setHivePort(Integer.parseInt(commandLine.getOptionValue(OptionFactory.CLI_HIVE_PORT)));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_PARTITION_KEY)){
            options.setHivePartitionKey(commandLine.getOptionValue(OptionFactory.CLI_HIVE_PARTITION_KEY));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_PARTITION_VALUE)){
            options.setHivePartitionValue(commandLine.getOptionValue(OptionFactory.CLI_HIVE_PARTITION_VALUE));
        }
        if(commandLine.hasOption(OptionFactory.CLI_MAP_COLUMN_HIVE)){
            options.setMapColumnHive(commandLine.getOptionValue(OptionFactory.CLI_MAP_COLUMN_HIVE));
        }
    }

    /**
     * 验证HCatalog选项
     * @param options
     * @throws InvalidOptionsException
     */
    protected void validateHCatalogOptions(SqoopxOptions options) throws InvalidOptionsException {
        String hCatTable = options.getHCatTableName();
        // hcatalog 方式
        if(hCatTable == null){
            if(options.getHCatHome() != null && !options.getHCatHome().equals(SqoopxOptions.getHCatHomeDefault())){
                log.warn("--hcatalog-home option will be ignored in non-HCatalog job");
            }
            if(options.getHCatDatabaseName() != null){
                log.warn("--hcatalog-database option will be ignored in non-HCatalog job");
            }
            if(options.getHCatStorageStanza() != null){
                log.warn("--hcatalog-storage-stanza option will be ignored in non-HCatalog job");
            }
        } else {
            if(StringUtils.isNotBlank(hCatTable) && HCatalogUtils.isHCatView(options)){
                throw new InvalidOptionsException("Reads/Writes from and to Views are not supported by HCatalog");
            }
            if(options.isExplicitInputDelims()){
                log.warn("Input field/record delimiter options are not used in HCatalog jobs unless the format is text. " +
                        "It is better to use --hive-import in those cases. For text formats");
            }
            if(options.isExplicitDelims()){
                log.warn("Output field/record delimiter options are not useful in HCatalog jobs for most of the output types except text based formats is text." +
                        "It is better to use --hive-import in those cases. For non text formats.");
            }
            // TODO
            if(options.getFileLayout() == FileLayout.AvroFile){
                throw new InvalidOptionsException("HCatalog job is not compatible with AVRO format option.");
            }
            if(options.getFileLayout() == FileLayout.SequenceFile){
                throw new InvalidOptionsException("HCatalog job is not compatible with Sequence format option.");
            }
            if(options.getFileLayout() == FileLayout.ParquetFile){
                throw new InvalidOptionsException("HCatalog job is not compatible with parquet format option.");
            }
            // 如果指定了hcatalog 分区键 但没有指定分区值
            if(options.getHCatalogPartitionKeys() != null && options.getHCatalogPartitionValues() == null){
                throw new InvalidOptionsException("Either both --hcatalog-partition-keys and --hcatalog-partition-values should " +
                        "be provided or both of these options should be omitted.");
            }
            // 如果指定了hcatalog 分区键
            if(options.getHCatalogPartitionKeys() != null){
                if(options.getHivePartitionKey() != null){
                    log.warn("Both --hcatalog-partition-keys and --hive-partition-keys options are provided. --hive-partition-keys option will be ignored.");
                }
                String[] keys = options.getHCatalogPartitionKeys().split(",");
                String[] values = options.getHCatalogPartitionValues().split(",");
                if(keys.length != values.length){
                    throw new InvalidOptionsException("Number of static partition keys provided don't match the number of partition values.");
                }
                for(int i=0; i<keys.length; i++){
                    if(keys[i].isEmpty()){
                        throw new InvalidOptionsException("Invalid HCatalog static partition key at position " + i);
                    }
                }
                for(int i=0; i<values.length; i++){
                    if(values[i].isEmpty()){
                        throw new InvalidOptionsException("Invalid HCatalog static partition value at position " + i);
                    }
                }
            } else { // 没有指定分区键 检查hive分区配置
                if(options.getHivePartitionKey() != null && options.getHivePartitionValue() == null){
                    throw new InvalidOptionsException("Either both --hive-partition-keys and --hive-partition-values should " +
                            "be provided or both of these options should be omitted.");
                }
            }
            if(options.isCreateHCatalogTable() && options.isDropAndCreateHCatalogTable()){
                throw new InvalidOptionsException("Options --create-hcatalog-table and --drop-and-create-hcatalog-table are mutually exclusive. Use any one of them");
            }
        }
    }

    /**
     * 获取hive的参数项
     * @param explictHiveImport  是否必须明确hive导入
     * @return
     */
    protected RelatedOptions getHiveOptions(boolean explictHiveImport){
        RelatedOptions options = new RelatedOptions("Hive arguments");
        if(explictHiveImport){
            options.addOption(OptionFactory.OPT_HIVE_IMPORT);
        }
        options.addOption(OptionFactory.OPT_HIVE_HOME);
        options.addOption(OptionFactory.OPT_HIVE_HOST);
        options.addOption(OptionFactory.OPT_HIVE_PORT);
        options.addOption(OptionFactory.OPT_HIVE_USERNAME);
        options.addOption(OptionFactory.OPT_HIVE_PASSWORD);
        options.addOption(OptionFactory.OPT_HIVE_OVERWRITE);
        options.addOption(OptionFactory.OPT_CREATE_HIVE_TABLE);
        options.addOption(OptionFactory.OPT_HIVE_TABLE);
        options.addOption(OptionFactory.OPT_HIVE_DATABASE);
        options.addOption(OptionFactory.OPT_HIVE_DROP_IMPORT_DELIMS);
        options.addOption(OptionFactory.OPT_HIVE_DELIMS_REPLACEMENT);
        options.addOption(OptionFactory.OPT_HIVE_PARTITION_KEY);
        options.addOption(OptionFactory.OPT_HIVE_PARTITION_VALUE);
        options.addOption(OptionFactory.OPT_EXTERNAL_TABLE_DIR);
        options.addOption(OptionFactory.OPT_MAP_COLUMN_HIVE);
        return options;
    }

    /**
     * @param commandLine
     * @param options
     */
    protected void applyHiveOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException {
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_HOME)){
            options.setHiveHome(commandLine.getOptionValue(OptionFactory.CLI_HIVE_HOME));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_HOST)){
            options.setHiveHost(commandLine.getOptionValue(OptionFactory.CLI_HIVE_HOST));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_PORT)){
            options.setHivePort(Integer.parseInt(commandLine.getOptionValue(OptionFactory.CLI_HIVE_PORT)));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_USERNAME)){
            options.setHiveUsername(commandLine.getOptionValue(OptionFactory.CLI_HIVE_USERNAME));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_PASSWORD)){
            options.setHivePassword(commandLine.getOptionValue(OptionFactory.CLI_HIVE_PASSWORD));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_IMPORT)){
            options.setHiveImport(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_OVERWRITE)){
            options.setOverwriteHiveTable(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_CREATE_HIVE_TABLE)){
            options.setFailIfHiveTableExists(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_TABLE)){
            options.setHiveTableName(commandLine.getOptionValue(OptionFactory.CLI_HIVE_TABLE));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_DATABASE)){
            options.setHiveDatabaseName(commandLine.getOptionValue(OptionFactory.CLI_HIVE_DATABASE));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_DROP_IMPORT_DELIMS)){
            options.setHiveDropDelims(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_DELIMS_REPLACEMENT)){
            options.setHiveDelimsReplacement(commandLine.getOptionValue(OptionFactory.CLI_HIVE_DELIMS_REPLACEMENT));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_PARTITION_KEY)){
            options.setHivePartitionKey(commandLine.getOptionValue(OptionFactory.CLI_HIVE_PARTITION_KEY));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HIVE_PARTITION_VALUE)){
            options.setHivePartitionValue(commandLine.getOptionValue(OptionFactory.CLI_HIVE_PARTITION_VALUE));
        }
        if(commandLine.hasOption(OptionFactory.CLI_MAP_COLUMN_HIVE)){
            options.setMapColumnHive(commandLine.getOptionValue(OptionFactory.CLI_MAP_COLUMN_HIVE));
        }
        if(commandLine.hasOption(OptionFactory.CLI_EXTERNAL_TABLE_DIR)){
            options.setHiveExternalTableDir(commandLine.getOptionValue(OptionFactory.CLI_EXTERNAL_TABLE_DIR));
        }
    }

    /**
     * @param options
     * @throws InvalidOptionsException
     */
    protected void validateHiveOptions(SqoopxOptions options) throws InvalidOptionsException {
        if(options.getHiveDelimsReplacement() != null && options.isHiveDropDelims()){
            throw new InvalidOptionsException("The --hive-drop-delims options conflicts with the --hive-delims-replacement options.");
        }
        String hCatTable = options.getHCatTableName();
        if(hCatTable != null && options.isHiveImport()){
            throw new InvalidOptionsException("The --hcatalog-table option conflicts with the --hive-import option.");
        }
        if(options.isHiveImport() && options.getFileLayout() == FileLayout.AvroFile){
            throw new InvalidOptionsException("Hive import is not compatible with importing into AVRO format.");
        }
        if(options.isHiveImport() && options.isFailIfHiveTableExists() && options.getFileLayout() == FileLayout.ParquetFile){
            throw new InvalidOptionsException("Hive import and create hive table is not compatible with importing into ParquetFile format.");
        }
//        if(options.isHiveImport() && options.getIncrementalMode().equals(IncrementalMode.DateLastModified)){
//            throw new InvalidOptionsException("");
//        }
//        if(options.isHiveImport() && options.isAppendMode() && !options.getIncrementalMode().equals(IncrementalMode.AppendRows)){
//            throw new InvalidOptionsException("Append mode for hive imports is not yet supported. Please remove the parameter --append-mode");
//        }
        String defaultWarehouse = "/user/hive/warehouse";
        if(options.isHiveImport() && (options.getWarehouseDir() != null && options.getWarehouseDir().startsWith(defaultWarehouse))
                || (options.getTargetDir() != null && options.getTargetDir().startsWith(defaultWarehouse))){
            // sqoopx是先将数据导入到一个独立的文件夹，然后再插入到hive，而不能直接指定到hive的default目录
        }
        // 如果不是用的hive导入，但是却指定了hive的外部表目录
        if(!options.isHiveImport() && !StringUtils.isBlank(options.getHiveExternalTableDir())){
            throw new InvalidOptionsException("Importing to external Hive table requires --hive-import parameter to be set.");
        }
        // 如果是hive导入，但是没有指定hive库的地址
        if(options.isHiveImport() && (options.getHiveHost() == null || options.getHivePort() == null)){
            throw new InvalidOptionsException("The host and port must be known with hive jdbc.");
        }
    }

    /**
     * @return
     */
    protected RelatedOptions getHBaseOptions(){
        RelatedOptions options = new RelatedOptions("HBase arguments");
        options.addOption(OptionFactory.OPT_HBASE_TABLE);
        options.addOption(OptionFactory.OPT_COLUMN_FAMILY);
        options.addOption(OptionFactory.OPT_HBASE_ROW_KEY);
        options.addOption(OptionFactory.OPT_HBASE_CREATE_TABLE);
        options.addOption(OptionFactory.OPT_HBASE_BULKLOAD);
        return options;
    }

    /**
     * @param commandLine
     * @param options
     */
    protected void applyHBaseOptions(CommandLine commandLine, SqoopxOptions options){
        if(commandLine.hasOption(OptionFactory.CLI_HBASE_TABLE)){
            options.setHBaseTable(commandLine.getOptionValue(OptionFactory.CLI_HBASE_TABLE));
        }
        if(commandLine.hasOption(OptionFactory.CLI_COLUMN_FAMILY)){
            options.setHBaseColFamily(commandLine.getOptionValue(OptionFactory.CLI_COLUMN_FAMILY));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HBASE_ROW_KEY)){
            options.setHBaseRowKeyColumn(commandLine.getOptionValue(OptionFactory.CLI_HBASE_ROW_KEY));
        }
        if(commandLine.hasOption(OptionFactory.CLI_HBASE_BULKLOAD)){
            options.setHBaseBulkLoadEnabled(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_HBASE_CREATE_TABLE)){
            options.setCreateHBaseTable(true);
        }
    }

    /**
     * @param options
     * @throws InvalidOptionsException
     */
    protected void validateHBaseOptions(SqoopxOptions options) throws InvalidOptionsException {
        if((options.getHBaseColFamily() != null && options.getHBaseTable() == null)
                || (options.getHBaseColFamily() == null && options.getHBaseTable() != null)){
            throw new InvalidOptionsException("Both --hbase-table and --column-family must be set together.");
        }
    }


    /**
     * @return
     */
    protected RelatedOptions getHCatImportOnlyOptions() {
        RelatedOptions options = new RelatedOptions("HCatalog import specific arguments");
        options.addOption(OptionFactory.OPT_CREATE_HCATALOG_TABLE);
        options.addOption(OptionFactory.OPT_DROP_AND_CREATE_HCATALOG_TABLE);
        options.addOption(OptionFactory.OPT_HCATALOG_STORAGE_STANZA);
        return options;
    }

    /**
     *
     * @return
     */
    protected RelatedOptions getAccumuloOptions(){
        RelatedOptions options = new RelatedOptions("Accumulo arguments");
        options.addOption(OptionFactory.OPT_ACCUMULO_TABLE);
        options.addOption(OptionFactory.OPT_ACCUMULO_COLUMN_FAMILY);
        options.addOption(OptionFactory.OPT_ACCUMULO_ROW_KEY);
        options.addOption(OptionFactory.OPT_ACCUMULO_VISIBILITY);
        options.addOption(OptionFactory.OPT_ACCUMULO_CREATE_TABLE);
        options.addOption(OptionFactory.OPT_ACCUMULO_BATCH_SIZE);
        options.addOption(OptionFactory.OPT_ACCUMULO_MAX_LATENCY);
        options.addOption(OptionFactory.OPT_ACCUMULO_ZOOKEEPERS);
        options.addOption(OptionFactory.OPT_ACCUMULO_INSTANCE);
        options.addOption(OptionFactory.OPT_ACCUMULO_USER);
        options.addOption(OptionFactory.OPT_ACCUMULO_PASSWORD);
        return options;
    }

    /**
     * @param className
     * @return
     * @throws InvalidOptionsException
     */
    protected Class getClassByName(String className) throws InvalidOptionsException {
        try {
            return Class.forName(className, true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new InvalidOptionsException(e);
        }
    }
}
