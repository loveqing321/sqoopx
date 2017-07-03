package com.deppon.hadoop.sqoopx.core.options;

import com.deppon.hadoop.sqoopx.core.cli.OptionFactory;
import com.deppon.hadoop.sqoopx.core.cli.RelatedOptions;
import com.deppon.hadoop.sqoopx.core.cli.ToolOptions;
import com.deppon.hadoop.sqoopx.core.exception.InvalidOptionsException;
import com.deppon.hadoop.sqoopx.core.jdbc.DBType;
import org.apache.commons.cli.CommandLine;

/**
 * 导出参数项配置器
 * 目前的导出功能只支持到 HCatTable -> 关系数据库的导出
 * Created by meepai on 2017/6/19.
 */
public class ExportOptionsConfigurer extends BaseOptionsConfigurer {

    /**
     *
     * @param options
     * @throws InvalidOptionsException
     */
    public void validateOptions(SqoopxOptions options) throws InvalidOptionsException {
        validateExportOptions(options);
        validateOutputFormatOptions(options);
        validateCommonOptions(options);
        validateHCatalogOptions(options);
        validateDirectExportOptions(options);
    }

    /**
     * 导出功能，必须要指定
     * 1. 导出的目标表或者目标存储过程
     * 2. 导出的源表 HCatalogTable
     * 3. 生成代码的文件夹位置
     * 4. 如果指定已有的jar文件，那么必须要指定实现Record类，且不能运行在update mode。
     * 5. 如果指定了分段表，那么分段表不能与目标表一致
     * 6. 如果指定了清空分段表，则必须指定分段表名
     * 7. 如果指定了存储过程，那么不能够启用分段表，不能够运行在update mode，不能够指定目标表
     *
     * 必须有的属性为
     * --table or --call
     * --export-dir or --hcatalog-table
     *
     * @param options
     * @throws InvalidOptionsException
     */
    public void validateExportOptions(SqoopxOptions options) throws InvalidOptionsException {
        if(options.getTableName() == null && options.getCall() == null){
            throw new InvalidOptionsException("Export requires a --table or --call arguments");
        }
        if(options.getExportDir() == null && options.getHCatTableName() == null){
            throw new InvalidOptionsException("Export requires a --export-dir or --hcatalog-table arguments");
        }
        if(options.getExistingJarName() != null){
            // 如果指定了已有的jar文件，那么必须指定表映射的实体Record类
            if(options.getClassName() == null){
                throw new InvalidOptionsException("Jar specified with --jar-file, but no class specified with --class-name.");
            }
            if(options.getUpdateKeyCol() != null){
                throw new InvalidOptionsException("Jar cannot be specified with --jar-file when export is running in update mode.");
            }
        }
        if(options.getStagingTableName() != null && options.getStagingTableName().equalsIgnoreCase(options.getTableName())){
            throw new InvalidOptionsException("Staging table cannot be the same as the destination table.");
        }
        if(options.isClearStagingTable() && options.getStagingTableName() == null){
            throw new InvalidOptionsException("Option to clear staging table is specified but staging table name is null.");
        }
        // 如果指定了存储过程
        if(options.getCall() != null){
            if(options.getStagingTableName() != null) {
                throw new InvalidOptionsException("Option to clear staging table is specified but staging table name is null.");
            }
            if(options.getUpdateKeyCol() != null){
                throw new InvalidOptionsException("Option to call a stored procedure can't be used in update mode.");
            }
            if(options.getTableName() != null){
                throw new InvalidOptionsException("Can't specify --call and --table.");
            }
        }
    }

    /**
     * 验证直接导出选项
     * @param options
     * @throws InvalidOptionsException
     */
    public void validateDirectExportOptions(SqoopxOptions options) throws InvalidOptionsException {
        if(options.isDirectMode()){
            // 所要导出的库不支持直接导出
            DBType type = DBType.from(options);
            if(type == null || !type.hasDirectConnector){
                throw new InvalidOptionsException("Export db use --direct, but it not support direct mode");
            }
        }
    }

    /**
     * 命令行到SqoopxOptions对象的解析
     * @param commandLine
     * @param options
     * @throws InvalidOptionsException
     */
    public void applyOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException {
        // common
        applyCommonOptions(commandLine, options);

        if(commandLine.hasOption(OptionFactory.CLI_DIRECT)){
            options.setDirectMode(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_BATCH)){
            options.setBatchMode(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_TABLE)){
            options.setTableName(commandLine.getOptionValue(OptionFactory.CLI_TABLE));
        }
        if(commandLine.hasOption(OptionFactory.CLI_COLUMNS)){
            String[] cols = commandLine.getOptionValue(OptionFactory.CLI_COLUMNS).split(",");
            for(int i=0; i<cols.length; i++){
                cols[i] = cols[i].trim();
            }
            options.setColumns(cols);
        }
        if(commandLine.hasOption(OptionFactory.CLI_NUM_MAPPERS)){
            options.setNumMappers(Integer.parseInt(commandLine.getOptionValue(OptionFactory.CLI_NUM_MAPPERS)));
        }
        if(commandLine.hasOption(OptionFactory.CLI_MAPREDUCE_JOB_NAME)){
            options.setMapreduceJobName(commandLine.getOptionValue(OptionFactory.CLI_MAPREDUCE_JOB_NAME));
        }
        if(commandLine.hasOption(OptionFactory.CLI_EXPORT_DIR)){
            options.setExportDir(commandLine.getOptionValue(OptionFactory.CLI_EXPORT_DIR));
        }
        if(commandLine.hasOption(OptionFactory.CLI_JAR_FILE)){
            options.setExistingJarName(commandLine.getOptionValue(OptionFactory.CLI_JAR_FILE));
        }
        if(commandLine.hasOption(OptionFactory.CLI_UPDATE_KEY)){
            options.setUpdateKeyCol(commandLine.getOptionValue(OptionFactory.CLI_UPDATE_KEY));
        }
        if(commandLine.hasOption(OptionFactory.CLI_STAGING_TABLE)){
            options.setStagingTableName(commandLine.getOptionValue(OptionFactory.CLI_STAGING_TABLE));
        }
        if(commandLine.hasOption(OptionFactory.CLI_CLEAR_STAGING_TABLE)){
            options.setClearStagingTable(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_CALL)){
            options.setCall(commandLine.getOptionValue(OptionFactory.CLI_CALL));
        }
        applyValidationOptions(commandLine, options);
        applyNewUpdateOptions(commandLine, options);
        applyInputFormatOptions(commandLine, options);
        applyOutputFormatOptions(commandLine, options);
        applyCodeGenOptions(commandLine, options, false);
        applyHCatalogOptions(commandLine, options);
    }

    /**
     * @param commandLine
     * @param options
     * @throws InvalidOptionsException
     */
    private void applyNewUpdateOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException {
        if(commandLine.hasOption(OptionFactory.CLI_UPDATE_MODE)){
            String updateType = commandLine.getOptionValue(OptionFactory.CLI_UPDATE_MODE);
            if("updateonly".equalsIgnoreCase(updateType)){
                options.setUpdateMode(SqoopxOptions.UpdateMode.UpdateOnly);
            } else if("allowinsert".equalsIgnoreCase(updateType)){
                options.setUpdateMode(SqoopxOptions.UpdateMode.AllowInsert);
            } else {
                throw new InvalidOptionsException("Unknown update mode: " + updateType);
            }
        }
    }

    /**
     * 命令行所有参数项构建
     * @param options
     */
    public void configureOptions(ToolOptions options) {
        // common 部分
        options.addOptionsGroup(getCommonOptions());
        // export 部分
        options.addOptionsGroup(getExportOptions());
        // validation 部分
        options.addOptionsGroup(getValidationOption());
        // inputFormat 部分 输入分割
        options.addOptionsGroup(getInputFormatOptions());
        // codeGen 部分
        options.addOptionsGroup(getCodeGenOptions(false));
        // hcatalog 部分
        options.addOptionsGroup(getHCatalogOptions());
    }

    /**
     * 导出选项
     * @return
     */
    protected RelatedOptions getExportOptions(){
        RelatedOptions exportOptions = new RelatedOptions("Export control arguments");

        exportOptions.addOption(OptionFactory.OPT_DIRECT);
        exportOptions.addOption(OptionFactory.OPT_TABLE);
        exportOptions.addOption(OptionFactory.OPT_COLUMNS);
        exportOptions.addOption(OptionFactory.OPT_MAPREDUCE_JOB_NAME);
        exportOptions.addOption(OptionFactory.OPT_NUM_MAPPERS);
        exportOptions.addOption(OptionFactory.OPT_EXPORT_DIR);
        exportOptions.addOption(OptionFactory.OPT_UPDATE_KEY);
        exportOptions.addOption(OptionFactory.OPT_STAGING_TABLE);
        exportOptions.addOption(OptionFactory.OPT_CLEAR_STAGING_TABLE);
        exportOptions.addOption(OptionFactory.OPT_BATCH);
        exportOptions.addOption(OptionFactory.OPT_UPDATE_MODE);
        exportOptions.addOption(OptionFactory.OPT_CALL);

        return exportOptions;
    }


}
