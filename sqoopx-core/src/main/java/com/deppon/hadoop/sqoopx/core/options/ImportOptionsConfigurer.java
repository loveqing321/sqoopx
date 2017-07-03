package com.deppon.hadoop.sqoopx.core.options;

import com.deppon.hadoop.sqoopx.core.cli.OptionFactory;
import com.deppon.hadoop.sqoopx.core.cli.RelatedOptions;
import com.deppon.hadoop.sqoopx.core.cli.ToolOptions;
import com.deppon.hadoop.sqoopx.core.exception.InvalidOptionsException;
import com.deppon.hadoop.sqoopx.core.file.FileLayout;
import org.apache.commons.cli.CommandLine;

/**
 * 导入的配置项
 * Created by meepai on 2017/6/19.
 */
public class ImportOptionsConfigurer extends BaseOptionsConfigurer {

    private boolean allTables;

    public ImportOptionsConfigurer(){
        this.allTables = false;
    }

    public void validateOptions(SqoopxOptions options) throws InvalidOptionsException {
        validateImportOptions(options);
//        validateIncrementalOptions(options);
        validateCommonOptions(options);
        validateCodeGenOptions(options);
        validateOutputFormatOptions(options);
        validateHBaseOptions(options);
        validateHiveOptions(options);
        validateHCatalogOptions(options);
//        validateAccumuloOptions(options);
    }

    public void applyOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException {
        applyCommonOptions(commandLine, options);
//        applyIncrementalOptions(commandLine, options);
        applyHiveOptions(commandLine, options);
        applyOutputFormatOptions(commandLine, options);
        applyInputFormatOptions(commandLine, options);
        applyCodeGenOptions(commandLine, options, allTables);
        applyHBaseOptions(commandLine, options);
        applyHCatalogOptions(commandLine, options);
//        applyAccumuloOptions(commandLine, options);
        applyImportOptions(commandLine, options);
    }

    public void configureOptions(ToolOptions options) {
        options.addOptionsGroup(getCommonOptions());
        options.addOptionsGroup(getImportOptions());
        options.addOptionsGroup(getValidationOption());
        options.addOptionsGroup(getOutputFormatOptions());
        options.addOptionsGroup(getInputFormatOptions());
        options.addOptionsGroup(getHiveOptions(true));
        options.addOptionsGroup(getHBaseOptions());
        options.addOptionsGroup(getHCatalogOptions());
        options.addOptionsGroup(getHCatImportOnlyOptions());
        options.addOptionsGroup(getAccumuloOptions());
        options.addOptionsGroup(getCodeGenOptions(allTables));
    }

    private RelatedOptions getImportOptions() {
        RelatedOptions options = new RelatedOptions("Import control arguments");
        options.addOption(OptionFactory.OPT_DIRECT);
        if(!allTables){
            options.addOption(OptionFactory.OPT_TABLE);
            options.addOption(OptionFactory.OPT_COLUMNS);
            options.addOption(OptionFactory.OPT_SPLIT_BY);
            options.addOption(OptionFactory.OPT_SPLIT_LIMIT);
            options.addOption(OptionFactory.OPT_WHERE);
            options.addOption(OptionFactory.OPT_APPEND);
            options.addOption(OptionFactory.OPT_DELETE);
            options.addOption(OptionFactory.OPT_TARGET_DIR);
            options.addOption(OptionFactory.OPT_QUERY);
            options.addOption(OptionFactory.OPT_BOUNDARY_QUERY);
            options.addOption(OptionFactory.OPT_MERGE_KEY);
        }
        options.addOption(OptionFactory.OPT_WAREHOUSE_DIR);
        options.addOption(OptionFactory.OPT_AS_SEQUENCEFILE);
        options.addOption(OptionFactory.OPT_AS_TEXTFILE);
        options.addOption(OptionFactory.OPT_AS_AVRODATAFILE);
        options.addOption(OptionFactory.OPT_AS_PARQUETFILE);
        options.addOption(OptionFactory.OPT_NUM_MAPPERS);
        options.addOption(OptionFactory.OPT_MAPREDUCE_JOB_NAME);
        options.addOption(OptionFactory.OPT_COMPRESS);
        options.addOption(OptionFactory.OPT_COMPRESSION_CODEC);
        options.addOption(OptionFactory.OPT_DIRECT_SPLIT_SIZE);
        options.addOption(OptionFactory.OPT_INLINE_LOB_LIMIT);
        options.addOption(OptionFactory.OPT_FETCH_SIZE);
        options.addOption(OptionFactory.OPT_AUTORESET_TO_ONE_MAPPER);
        return options;
    }

    private void applyImportOptions(CommandLine commandLine, SqoopxOptions options) throws InvalidOptionsException {
        if(commandLine.hasOption(OptionFactory.CLI_DIRECT)){
            options.setDirectMode(true);
        }
        if(!allTables){
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
            if(commandLine.hasOption(OptionFactory.CLI_SPLIT_BY)){
                options.setSplitByCol(commandLine.getOptionValue(OptionFactory.CLI_SPLIT_BY));
            }
            if(commandLine.hasOption(OptionFactory.CLI_SPLIT_LIMIT)){
                options.setSplitLimit(Integer.parseInt(commandLine.getOptionValue(OptionFactory.CLI_SPLIT_LIMIT)));
            }
            if(commandLine.hasOption(OptionFactory.CLI_WHERE)){
                options.setWhereClause(commandLine.getOptionValue(OptionFactory.CLI_WHERE));
            }
            if(commandLine.hasOption(OptionFactory.CLI_TARGET_DIR)){
                options.setTargetDir(commandLine.getOptionValue(OptionFactory.CLI_TARGET_DIR));
            }
            if(commandLine.hasOption(OptionFactory.CLI_APPEND)){
                options.setAppendMode(true);
            }
            if(commandLine.hasOption(OptionFactory.CLI_DELETE)){
                options.setDeleteMode(true);
            }
            if(commandLine.hasOption(OptionFactory.CLI_QUERY)){
                options.setSqlQuery(commandLine.getOptionValue(OptionFactory.CLI_QUERY));
            }
            if(commandLine.hasOption(OptionFactory.CLI_BOUNDARY_QUERY)){
                options.setBoundaryQuery(commandLine.getOptionValue(OptionFactory.CLI_BOUNDARY_QUERY));
            }
            if(commandLine.hasOption(OptionFactory.CLI_MERGE_KEY)){
                options.setMergeKeyCol(commandLine.getOptionValue(OptionFactory.CLI_MERGE_KEY));
            }
        }
        if(commandLine.hasOption(OptionFactory.CLI_WAREHOUSE_DIR)){
            options.setWarehouseDir(commandLine.getOptionValue(OptionFactory.CLI_WAREHOUSE_DIR));
        }
        if(commandLine.hasOption(OptionFactory.CLI_AS_SEQUENCEFILE)){
            options.setFileLayout(FileLayout.SequenceFile);
        }
        if(commandLine.hasOption(OptionFactory.CLI_AS_TEXTFILE)){
            options.setFileLayout(FileLayout.TextFile);
        }
        if(commandLine.hasOption(OptionFactory.CLI_AS_AVRODATAFILE)){
            options.setFileLayout(FileLayout.AvroFile);
        }
        if(commandLine.hasOption(OptionFactory.CLI_AS_PARQUETFILE)){
            options.setFileLayout(FileLayout.ParquetFile);
        }
        if(commandLine.hasOption(OptionFactory.CLI_NUM_MAPPERS)){
            options.setNumMappers(Integer.parseInt(commandLine.getOptionValue(OptionFactory.CLI_NUM_MAPPERS)));
        }
        if(commandLine.hasOption(OptionFactory.CLI_MAPREDUCE_JOB_NAME)){
            options.setMapreduceJobName(commandLine.getOptionValue(OptionFactory.CLI_MAPREDUCE_JOB_NAME));
        }
        if(commandLine.hasOption(OptionFactory.CLI_COMPRESS)){
            options.setUseCompression(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_COMPRESSION_CODEC)){
            options.setCompressionCodes(commandLine.getOptionValue(OptionFactory.CLI_COMPRESSION_CODEC));
        }
        if(commandLine.hasOption(OptionFactory.CLI_DIRECT_SPLIT_SIZE)){
            options.setDirectSplitSize(Long.parseLong(commandLine.getOptionValue(OptionFactory.CLI_DIRECT_SPLIT_SIZE)));
        }
        if(commandLine.hasOption(OptionFactory.CLI_INLINE_LOB_LIMIT)){
            options.setInlineLobLimit(Long.parseLong(commandLine.getOptionValue(OptionFactory.CLI_INLINE_LOB_LIMIT)));
        }
        if(commandLine.hasOption(OptionFactory.CLI_FETCH_SIZE)){
            options.setFetchSize(new Integer(commandLine.getOptionValue(OptionFactory.CLI_FETCH_SIZE)));
        }
        if(commandLine.hasOption(OptionFactory.CLI_JAR_FILE)){
            options.setExistingJarName(commandLine.getOptionValue(OptionFactory.CLI_JAR_FILE));
        }
        if(commandLine.hasOption(OptionFactory.CLI_AUTORESET_TO_ONE_MAPPER)){
            options.setAutoResetToOneMapper(true);
        }
        if(commandLine.hasOption(OptionFactory.CLI_ESCAPE_MAPPING_COLUMN_NAMES)){
            options.setEscapMappingColumnNamesEnabled(Boolean.parseBoolean(commandLine.getOptionValue(OptionFactory.CLI_ESCAPE_MAPPING_COLUMN_NAMES)));
        }
    }

    private void validateImportOptions(SqoopxOptions options) throws InvalidOptionsException {
        if(!allTables && options.getTableName() == null && options.getSqlQuery() == null){
            throw new InvalidOptionsException("--table or --query is required for import (or use import-all-tables.)");
        }
        if(options.getExistingJarName() != null && options.getClassName() == null){
            throw new InvalidOptionsException("jar specified with -jar-file, but no class specified with --class-name.");
        }
        if(options.getTargetDir() != null && options.getWarehouseDir() != null){
            throw new InvalidOptionsException("--target-dir with --warehouse-dir are incompatible options.");
        }
        if(options.getTableName() != null && options.getSqlQuery() != null){
            throw new InvalidOptionsException("Cannot specify --query and --table together.");
        }
//        if(options.getSqlQuery() != null && options.getTargetDir() == null && options.getHBaseTable() == null
//                && options.getHCatTableName() == null && options.getAccumuloTable() == null){
//            throw new InvalidOptionsException("Must specify destination with --target-dir");
//        }
        if(options.getSqlQuery() != null && options.isHiveImport() && options.getHiveTableName() == null){
            throw new InvalidOptionsException("When importing a query to Hive, you must specify --hive-table.");
        }
        if(options.getSqlQuery() != null && options.getNumMappers() > 1 && options.getSplitByCol() == null){
            throw new InvalidOptionsException("When importing query results in parallel, you must specify --split-by.");
        }
        if(options.isDirectMode()){

        }
        if(options.getSqlQuery() != null && options.isValidationEnabled()){
            throw new InvalidOptionsException("Validation is not supported for free from query but single table only.");
        }
        if(options.getWhereClause() != null && options.isValidationEnabled()){
            throw new InvalidOptionsException("Validation is not supported for where clause but single table only.");
        }
        if((options.getTargetDir() != null || options.getWarehouseDir() != null) && options.getHCatTableName() != null){
            throw new InvalidOptionsException("--hcatalog-table cannot be used --warehouse-dir or --target-dir options.");
        }
    }

}
