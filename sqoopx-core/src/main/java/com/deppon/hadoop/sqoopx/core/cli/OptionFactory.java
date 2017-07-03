package com.deppon.hadoop.sqoopx.core.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;

/**
 * Created by meepai on 2017/6/22.
 */
public class OptionFactory {

    public static final String CLI_CONNECT = "connect";

    public static final String CLI_CONN_MANAGER = "connection-manager";

    public static final String CLI_CONN_PARAM_FILE = "connection-param-file";

    public static final String CLI_DRIVER = "driver";

    public static final String CLI_USERNAME = "username";

    public static final String CLI_PASSWORD = "password";

    public static final String CLI_PASSWORD_FILE = "password-file";

    public static final String CLI_PASSWORD_ALIAS = "password-alias";

    public static final String CLI_P = "P";

    public static final String CLI_HADOOP_MAPRED_HOME = "hadoop-mapred-home";

    public static final String CLI_HADOOP_HOME = "hadoop-home";

    public static final String CLI_SKIP_DIST_CACHE = "skip-dist-cache";

    public static final String CLI_VERBOSE = "verbose";

    public static final String CLI_HELP = "help";

    public static final String CLI_TEMPORARY_ROOTDIR = "temporary-rootdir";

    public static final String CLI_EXTENSION_CLASSES = "extension-classes";

    public static final String CLI_METADATA_TRANSACTION_ISOLATION_LEVEL = "metadata-transaction-isolation-level";

    public static final String CLI_THROW_ON_ERROR = "throw-on-error";

    public static final String CLI_RELAXED_ISOLATION = "relaxed-isolation";

    public static final String CLI_ORACLE_ESCAPING_DISABLED = "oracle-escaping-disabled";

    public static final String CLI_HIVE_IMPORT = "hive-import";

    public static final String CLI_HCATALOG_TABLE = "hcatalog-table";

    public static final String CLI_HCATALOG_DATABASE = "hcatalog-database";

    public static final String CLI_HCATALOG_STORAGE_STANZA = "hcatalog-storage-stanza";

    public static final String CLI_CREATE_HCATALOG_TABLE = "create-hcatalog-table";

    public static final String CLI_DROP_AND_CREATE_HCATALOG_TABLE = "drop-and-create-hcatalog-table";

    public static final String CLI_HIVE_HOME = "hive-home";

    public static final String CLI_HIVE_HOST = "hive-host";

    public static final String CLI_HIVE_PORT = "hive-port";

    public static final String CLI_HIVE_USERNAME = "hive-username";

    public static final String CLI_HIVE_PASSWORD = "hive-password";

    public static final String CLI_HCATALOG_HOME = "hcatalog-home";

    public static final String CLI_HIVE_OVERWRITE = "hive-overwrite";

    public static final String CLI_CREATE_HIVE_TABLE = "create-hive-table";

    public static final String CLI_HIVE_TABLE = "hive-table";

    public static final String CLI_HIVE_DATABASE = "hive-database";

    public static final String CLI_HIVE_DROP_IMPORT_DELIMS = "hive-drop-import-delims";

    public static final String CLI_HIVE_DELIMS_REPLACEMENT = "hive-delims-replacement";

    public static final String CLI_HIVE_PARTITION_KEY = "hive-partition-key";

    public static final String CLI_HIVE_PARTITION_VALUE = "hive-partition-value";

    public static final String CLI_EXTERNAL_TABLE_DIR = "external-table-dir";

    public static final String CLI_MAP_COLUMN_HIVE = "map-column-hive";

    public static final String CLI_HCATALOG_PARTITION_KEYS = "hcatalog-partition-keys";

    public static final String CLI_HCATALOG_PARTITION_VALUES = "hcatalog-partition-values";

    public static final String CLI_ACCUMULO_TABLE = "accumulo-table";

    public static final String CLI_ACCUMULO_COLUMN_FAMILY = "accumulo-column-family";

    public static final String CLI_ACCUMULO_ROW_KEY = "accumulo-row-key";

    public static final String CLI_ACCUMULO_VISIBILITY = "accumulo-visibility";

    public static final String CLI_ACCUMULO_CREATE_TABLE = "accumulo-create-table";

    public static final String CLI_ACCUMULO_BATCH_SIZE = "accumulo-batch-size";

    public static final String CLI_ACCUMULO_MAX_LATENCY = "accumulo-max-latency";

    public static final String CLI_ACCUMULO_ZOOKEEPERS = "accumulo-zookeepers";

    public static final String CLI_ACCUMULO_INSTANCE = "accumulo-instance";

    public static final String CLI_ACCUMULO_USER = "accumulo-user";

    public static final String CLI_ACCUMULO_PASSWORD = "accumulo-password";

    public static final String CLI_META_CONNECT = "meta-connect";

    public static final String CLI_JOB_CREATE = "create";

    public static final String CLI_JOB_DELETE = "delete";

    public static final String CLI_JOB_SHOW = "show";

    public static final String CLI_JOB_EXEC = "exec";

    public static final String CLI_JOB_LIST = "list";

    public static final String CLI_FIELDS_TERMINATED_BY = "fields-terminated-by";

    public static final String CLI_LINES_TERMINATED_BY = "lines-terminated-by";

    public static final String CLI_OPTIONALLY_ENCLOSED_BY = "optionally-enclosed-by";

    public static final String CLI_ENCLOSED_BY = "enclosed-by";

    public static final String CLI_ESCAPED_BY = "escaped-by";

    public static final String CLI_MYSQL_DELIMITERS = "mysql-delimiters";

    public static final String CLI_INPUT_FIELDS_TERMINATED_BY = "input-fields-terminated-by";

    public static final String CLI_INPUT_LINES_TERMINATED_BY = "input-lines-terminated-by";

    public static final String CLI_INPUT_OPTIONALLY_ENCLOSED_BY = "input-optionally-enclosed-by";

    public static final String CLI_INPUT_ENCLOSED_BY = "input-enclosed-by";

    public static final String CLI_INPUT_ESCAPED_BY = "input-escaped-by";

    public static final String CLI_OUT_DIR = "outdir";

    public static final String CLI_BIN_DIR = "bindir";

    public static final String CLI_PACKAGE_NAME = "package-name";

    public static final String CLI_NULL_STRING = "null-string";

    public static final String CLI_INPUT_NULL_STRING = "input-null-string";

    public static final String CLI_NULL_NON_STRING = "null-non-string";

    public static final String CLI_INPUT_NULL_NON_STRING = "input-null-non-string";

    public static final String CLI_MAP_COLUMN_JAVA = "map-column-java";

    public static final String CLI_ESCAPE_MAPPING_COLUMN_NAMES = "escape-mapping-column-names";

    public static final String CLI_CLASS_NAME = "class-name";

    public static final String CLI_JAR_FILE = "jar-file";

    public static final String CLI_TABLE = "table";

    public static final String CLI_DIRECT = "direct";

    public static final String CLI_COLUMNS = "columns";

    public static final String CLI_MAPREDUCE_JOB_NAME = "mapreduce-job-name";

    public static final String CLI_NUM_MAPPERS = "num-mappers";

    public static final String CLI_EXPORT_DIR = "export-dir";

    public static final String CLI_UPDATE_KEY = "update-key";

    public static final String CLI_STAGING_TABLE = "staging-table";

    public static final String CLI_CLEAR_STAGING_TABLE = "clear-staging-table";

    public static final String CLI_BATCH = "batch";

    public static final String CLI_UPDATE_MODE = "update-mode";

    public static final String CLI_CALL = "call";

    public static final String CLI_QUERY = "query";

    public static final String CLI_FETCH_SIZE = "fetch-size";

    public static final String CLI_HBASE_TABLE = "hbase-table";

    public static final String CLI_COLUMN_FAMILY = "column-family";

    public static final String CLI_HBASE_ROW_KEY = "hbase-row-key";

    public static final String CLI_HBASE_CREATE_TABLE = "hbase-create-table";

    public static final String CLI_HBASE_BULKLOAD = "hbase-bulkload";

    public static final String CLI_VALIDATE = "validate";

    public static final String CLI_VALIDATOR = "validator";

    public static final String CLI_VALIDATION_THRESHOLD = "validation-threshold";

    public static final String CLI_VALIDATION_FAILUREHANDLER = "validation-failurehandler";

    public static final String CLI_SPLIT_BY = "split-by";

    public static final String CLI_SPLIT_LIMIT = "split-limit";

    public static final String CLI_WHERE = "where";

    public static final String CLI_APPEND = "append";

    public static final String CLI_DELETE = "delete";


    public static final String CLI_TARGET_DIR = "target-dir";

    public static final String CLI_BOUNDARY_QUERY = "boundary-query";

    public static final String CLI_MERGE_KEY = "merge-key";

    public static final String CLI_WAREHOUSE_DIR = "warehouse-dir";

    public static final String CLI_AS_SEQUENCEFILE = "as-sequencefile";

    public static final String CLI_AS_TEXTFILE = "as-textfile";

    public static final String CLI_AS_AVRODATAFILE = "as-avrodatafile";

    public static final String CLI_AS_PARQUETFILE = "as-parquetfile";

    public static final String CLI_COMPRESS = "compress";

    public static final String CLI_COMPRESSION_CODEC = "compression-codec";

    public static final String CLI_DIRECT_SPLIT_SIZE = "direct-split-size";

    public static final String CLI_INLINE_LOB_LIMIT = "inline-Lob-limit";

    public static final String CLI_AUTORESET_TO_ONE_MAPPER = "autoreset-to-one-mapper";


    // 连接参数
    public static final Option OPT_CONNECT = new Option(null, CLI_CONNECT, true, "Specify JDBC connect string");
    //
    public static final Option OPT_CONN_MANAGER = new Option(null, CLI_CONN_MANAGER, true, "Specify connection manager class name");
    //
    public static final Option OPT_CONN_PARAM_FILE = new Option(null, CLI_CONN_PARAM_FILE, true, "Specify connection parameters file");
    //
    public static final Option OPT_DRIVER = new Option(null, CLI_DRIVER, true, "Manually specify JDBC driver class to use");
    //
    public static final Option OPT_USERNAME = new Option(null, CLI_USERNAME, true, "Set authentication username");
    //
    public static final Option OPT_PASSWORD = new Option(null, CLI_PASSWORD, true, "Set authentication password");
    //
    public static final Option OPT_PASSWORD_FILE = new Option(null, CLI_PASSWORD_FILE, true, "Set authentication password file path");
    //
    public static final Option OPT_PASSWORD_ALIAS = new Option(null, CLI_PASSWORD_ALIAS, true, "Credential provider password alias");
    //
    public static final Option OPT_P = new Option(CLI_P, "Read password from console");
    //
    public static final Option OPT_HADOOP_MAPRED_HOME = new Option(null, CLI_HADOOP_MAPRED_HOME, true, "Override $HADOOP_MAPRED_HOME_ARG");
    //
    public static final Option OPT_HADOOP_HOME = new Option(null, CLI_HADOOP_HOME, true, "Override $HADOOP_HOME_ARG");
    //
    public static final Option OPT_SKIP_DIST_CACHE = new Option(null, CLI_SKIP_DIST_CACHE, false, "Skip coping jars to distributed cache");
    //
    public static final Option OPT_VERBOSE = new Option(null, CLI_VERBOSE, false, "Print more information while working");
    //
    public static final Option OPT_HELP = new Option(null, CLI_HELP, false, "Print usage instructions");
    //
    public static final Option OPT_TEMPORARY_ROOTDIR = new Option(null, CLI_TEMPORARY_ROOTDIR, true, "Defines the temporary root directory for the import");
    //
    public static final Option OPT_EXTENSION_CLASSES = new Option(null, CLI_EXTENSION_CLASSES, true, "Define extension classes!");
    //
    public static final Option OPT_METADATA_TRANSACTION_ISOLATION_LEVEL = new Option(null, CLI_METADATA_TRANSACTION_ISOLATION_LEVEL, true, "Defines the transaction isolation level for metadata queries. For more details check java.sql.Connection javadoc or the JDBC specification");
    //
    public static final Option OPT_THROW_ON_ERROR = new Option(null, CLI_THROW_ON_ERROR, false, "Rethrow a RuntimeException on error occurred during the job");
    //
    public static final Option OPT_RELAXED_ISOLATION = new Option(null, CLI_RELAXED_ISOLATION, false, "Use read-uncommitted isolation for imports");
    //
    public static final Option OPT_ORACLE_ESCAPING_DISABLED = new Option(null, CLI_ORACLE_ESCAPING_DISABLED, true, "Disable the escaping mechanism of the Oracle/OraOop connection managers");
    //
    public static final Option OPT_HIVE_IMPORT = new Option(null, CLI_HIVE_IMPORT, false, "Import tables into Hive (Uses Hive's default delimiters if none are set.)");
    //
    public static final Option OPT_HCATALOG_TABLE = new Option(null, CLI_HCATALOG_TABLE, true, "HCatalog table name");
    //
    public static final Option OPT_HCATALOG_DATABASE = new Option(null, CLI_HCATALOG_DATABASE, true, "HCatalog database name");
    //
    public static final Option OPT_HCATALOG_STORAGE_STANZA = new Option(null, CLI_HCATALOG_STORAGE_STANZA, true, "HCatalog storage stanza");
    //
    public static final Option OPT_CREATE_HCATALOG_TABLE = new Option(null, CLI_CREATE_HCATALOG_TABLE, true, "Create HCatalog table");
    //
    public static final Option OPT_DROP_AND_CREATE_HCATALOG_TABLE = new Option(null, CLI_DROP_AND_CREATE_HCATALOG_TABLE, true, "Drop and create HCatalog table");
    //
    public static final Option OPT_HIVE_HOME = new Option(null, CLI_HIVE_HOME, true, "Override $HIVE_HOME");
    //
    public static final Option OPT_HIVE_HOST = new Option(null, CLI_HIVE_HOST, true, "Set hive host.");
    //
    public static final Option OPT_HIVE_PORT = new Option(null, CLI_HIVE_PORT, true, "Set hive port.");
    //
    public static final Option OPT_HIVE_USERNAME = new Option(null, CLI_HIVE_USERNAME, true, "Set hive username.");
    //
    public static final Option OPT_HIVE_PASSWORD = new Option(null, CLI_HIVE_PASSWORD, true, "Set hive password.");
    //
    public static final Option OPT_HCATALOG_HOME = new Option(null, CLI_HCATALOG_HOME, true, "Override $HCAT_HOME");
    //
    public static final Option OPT_HIVE_OVERWRITE = new Option(null, CLI_HIVE_OVERWRITE, true, "Overwrite existing data in the Hive table");
    //
    public static final Option OPT_CREATE_HIVE_TABLE = new Option(null, CLI_CREATE_HIVE_TABLE, true, "Fail if the target hive table exists");
    //
    public static final Option OPT_HIVE_TABLE = new Option(null, CLI_HIVE_TABLE, true, "Sets the table name to use when importing to hive");
    //
    public static final Option OPT_HIVE_DATABASE = new Option(null, CLI_HIVE_DATABASE, true, "Sets the database name to use when importing to hive");
    //
    public static final Option OPT_HIVE_DROP_IMPORT_DELIMS = new Option(null, CLI_HIVE_DROP_IMPORT_DELIMS, true, "Drop Hive record \\0x01 and row delimiters (\\n\\r) from imported string fields");
    //
    public static final Option OPT_HIVE_DELIMS_REPLACEMENT = new Option(null, CLI_HIVE_DELIMS_REPLACEMENT, true, "Replace Hive record \\0x01 and row delimiters (\\n\\r) from imported string fields");
    //
    public static final Option OPT_HIVE_PARTITION_KEY = new Option(null, CLI_HIVE_PARTITION_KEY, true, "Sets the partition key to use when importing to hive");
    //
    public static final Option OPT_HIVE_PARTITION_VALUE = new Option(null, CLI_HIVE_PARTITION_VALUE, true, "Sets the partition value to use when importing to hive");
    //
    public static final Option OPT_EXTERNAL_TABLE_DIR = new Option(null, CLI_EXTERNAL_TABLE_DIR, true, "Sets the external table location");
    //
    public static final Option OPT_MAP_COLUMN_HIVE = new Option(null, CLI_MAP_COLUMN_HIVE, true, "Override mapping for specific column to hive types.");
    //
    public static final Option OPT_HCATALOG_PARTITION_KEYS = new Option(null, CLI_HCATALOG_PARTITION_KEYS, true, "Sets the partition keys to use when importing to hive");
    //
    public static final Option OPT_HCATALOG_PARTITION_VALUES = new Option(null, CLI_HCATALOG_PARTITION_VALUES, true, "Sets the partition values to use when importing to hive");
    //
    public static final Option OPT_ACCUMULO_TABLE = new Option(null, CLI_ACCUMULO_TABLE, true, "Import to <table> in Accumulo");
    //
    public static final Option OPT_ACCUMULO_COLUMN_FAMILY = new Option(null, CLI_ACCUMULO_COLUMN_FAMILY, true, "Sets the target column family for the import");
    //
    public static final Option OPT_ACCUMULO_ROW_KEY = new Option(null, CLI_ACCUMULO_ROW_KEY, true, "Specifies which input column to use as the row key");
    //
    public static final Option OPT_ACCUMULO_VISIBILITY = new Option(null, CLI_ACCUMULO_VISIBILITY, true, "Visibility token to be applied to all rows imported");
    //
    public static final Option OPT_ACCUMULO_CREATE_TABLE = new Option(null, CLI_ACCUMULO_CREATE_TABLE, true, "If specified, create missing Accumulo tables");
    //
    public static final Option OPT_ACCUMULO_BATCH_SIZE = new Option(null, CLI_ACCUMULO_BATCH_SIZE, true, "Batch size in bytes");
    //
    public static final Option OPT_ACCUMULO_MAX_LATENCY = new Option(null, CLI_ACCUMULO_MAX_LATENCY, true, "Max write latency in milliseconds");
    //
    public static final Option OPT_ACCUMULO_ZOOKEEPERS = new Option(null, CLI_ACCUMULO_ZOOKEEPERS, true, "Comma-separated list of zookeepers (host:port)");
    //
    public static final Option OPT_ACCUMULO_INSTANCE = new Option(null, CLI_ACCUMULO_INSTANCE, true, "Accumulo instance name.");
    //
    public static final Option OPT_ACCUMULO_USER = new Option(null, CLI_ACCUMULO_USER, true, "Accumulo user name.");
    //
    public static final Option OPT_ACCUMULO_PASSWORD = new Option(null, CLI_ACCUMULO_PASSWORD, true, "Accumulo password.");
    //
    public static final Option OPT_META_CONNECT = new Option(null, CLI_META_CONNECT, true, "Specify JDBC connect string for the metastore");

    public static final OptionGroup JOB_GROUP = new OptionGroup();

    static {
        JOB_GROUP.addOption(new Option(null, CLI_JOB_CREATE, true, "Create a new saved job"));
        JOB_GROUP.addOption(new Option(null, CLI_JOB_DELETE, true, "Delete a saved job"));
        JOB_GROUP.addOption(new Option(null, CLI_JOB_SHOW, true, "Show the parameters for a saved job"));
        JOB_GROUP.addOption(new Option(null, CLI_JOB_EXEC, true, "Run a saved job"));
        JOB_GROUP.addOption(new Option(null, CLI_JOB_LIST, false, "List saved jobs"));
    }

    //
    public static final Option OPT_FIELDS_TERMINATED_BY = new Option(null, CLI_FIELDS_TERMINATED_BY, true, "Sets the field separator character");
    //
    public static final Option OPT_LINES_TERMINATED_BY = new Option(null, CLI_LINES_TERMINATED_BY, true, "Sets the end-of-line character");
    //
    public static final Option OPT_OPTIONALLY_ENCLOSED_BY = new Option(null, CLI_OPTIONALLY_ENCLOSED_BY, true, "Sets a field enclosing character");
    //
    public static final Option OPT_ENCLOSED_BY = new Option(null, CLI_ENCLOSED_BY, true, "Sets a required field enclosing character");
    //
    public static final Option OPT_ESCAPED_BY = new Option(null, CLI_ESCAPED_BY, true, "Sets the escape character");
    //
    public static final Option OPT_MYSQL_DELIMITERS = new Option(null, CLI_MYSQL_DELIMITERS, false, "Uses MySQL's default delimiter set: fields: ,  lines: \\n  escaped-by: \\  optionally-enclosed-by: '");
    //
    public static final Option OPT_INPUT_FIELDS_TERMINATED_BY = new Option(null, CLI_INPUT_FIELDS_TERMINATED_BY, true, "Sets the input field separator");
    //
    public static final Option OPT_INPUT_LINES_TERMINATED_BY = new Option(null, CLI_INPUT_LINES_TERMINATED_BY, true, "Sets the input end-of-line char");
    //
    public static final Option OPT_INPUT_OPTIONALLY_ENCLOSED_BY = new Option(null, CLI_INPUT_OPTIONALLY_ENCLOSED_BY, true, "Sets a field enclosing character");
    //
    public static final Option OPT_INPUT_ENCLOSED_BY = new Option(null, CLI_INPUT_ENCLOSED_BY, true, "Sets a required field encloser");
    //
    public static final Option OPT_INPUT_ESCAPED_BY = new Option(null, CLI_INPUT_ESCAPED_BY, true, "Sets the input escape character");
    //
    public static final Option OPT_OUT_DIR = new Option(null, CLI_OUT_DIR, true, "Output directory for generated code");
    //
    public static final Option OPT_BIN_DIR = new Option(null, CLI_BIN_DIR, true, "Output directory for compiled objects");
    //
    public static final Option OPT_PACKAGE_NAME = new Option(null, CLI_PACKAGE_NAME, true, "Put auto-generated classes in this package");
    //
    public static final Option OPT_NULL_STRING = new Option(null, CLI_NULL_STRING, true, "Null string representation");
    //
    public static final Option OPT_INPUT_NULL_STRING = new Option(null, CLI_INPUT_NULL_STRING, true, "Input null string representation");
    //
    public static final Option OPT_NULL_NON_STRING = new Option(null, CLI_NULL_NON_STRING, true, "Null non-string representation");
    //
    public static final Option OPT_INPUT_NULL_NON_STRING = new Option(null, CLI_INPUT_NULL_NON_STRING, true, "Input null non-string representation");
    //
    public static final Option OPT_MAP_COLUMN_JAVA = new Option(null, CLI_MAP_COLUMN_JAVA, true, "Override mapping for specific columns to java types");
    //
    public static final Option OPT_ESCAPE_MAPPING_COLUMN_NAMES = new Option(null, CLI_ESCAPE_MAPPING_COLUMN_NAMES, true, "Disable special characters escaping in column names");
    //
    public static final Option OPT_CLASS_NAME = new Option(null, CLI_CLASS_NAME, true, "Sets the generated class name. ");
    //
    public static final Option OPT_JAR_FILE = new Option(null, CLI_JAR_FILE, true, "Disable code generation; use specified jar");
    //
    public static final Option OPT_TABLE = new Option(null, CLI_TABLE, true, "Table to generate code for");
    //
    public static final Option OPT_DIRECT = new Option(null, CLI_DIRECT, true, "Use direct export fast path");
    //
    public static final Option OPT_COLUMNS = new Option(null, CLI_COLUMNS, true, "Columns to generate code for");
    //
    public static final Option OPT_MAPREDUCE_JOB_NAME = new Option(null, CLI_MAPREDUCE_JOB_NAME, true, "Set name for generated mapreduce job");
    //
    public static final Option OPT_NUM_MAPPERS = new Option("m", CLI_NUM_MAPPERS, true, "Use 'n' map tasks to export in parallel");
    //
    public static final Option OPT_EXPORT_DIR = new Option(null, CLI_EXPORT_DIR, true, "HDFS source path for the export");
    //
    public static final Option OPT_UPDATE_KEY = new Option(null, CLI_UPDATE_KEY, true, "Update records by specified key column");
    //
    public static final Option OPT_STAGING_TABLE = new Option(null, CLI_STAGING_TABLE, true, "Intermediate staging table");
    //
    public static final Option OPT_CLEAR_STAGING_TABLE = new Option(null, CLI_CLEAR_STAGING_TABLE, false, "Indicates that any data in staging table can be deleted");
    //
    public static final Option OPT_BATCH = new Option(null, CLI_BATCH, false, "Indicates underlying statements to be executed in batch mode");
    //
    public static final Option OPT_UPDATE_MODE = new Option(null, CLI_UPDATE_MODE, true, "Specifies how updates are performed when new rows are found with non-watching keys in database");
    //
    public static final Option OPT_CALL = new Option(null, CLI_CALL, true, "Populate the table using this stored procedure (one call per row)");
    //
    public static final Option OPT_QUERY = new Option("e", CLI_QUERY, true, "SQL 'statement' to get column names for");
    //
    public static final Option OPT_FETCH_SIZE = new Option(null, CLI_FETCH_SIZE, true, "Query fetch size!");
    //
    public static final Option OPT_HBASE_TABLE = new Option(null, CLI_HBASE_TABLE, true, "Import to <table> in HBase");
    //
    public static final Option OPT_COLUMN_FAMILY = new Option(null, CLI_COLUMN_FAMILY, true, "Sets the target column family for the import");
    //
    public static final Option OPT_HBASE_ROW_KEY = new Option(null, CLI_HBASE_ROW_KEY, true, "Specifies which input column to use as the row key");
    //
    public static final Option OPT_HBASE_CREATE_TABLE = new Option(null, CLI_HBASE_CREATE_TABLE, true, "If specified, create missing HBase tables");
    //
    public static final Option OPT_HBASE_BULKLOAD = new Option(null, CLI_HBASE_BULKLOAD, true, "Sets hbase bulk load.");
    //
    public static final Option OPT_VALIDATE = new Option(null, CLI_VALIDATE, false, "Validate the copy using the configured validator");
    //
    public static final Option OPT_VALIDATOR = new Option(null, CLI_VALIDATOR, true, "Fully qualified class name for the Validator");
    //
    public static final Option OPT_VALIDATION_THRESHOLD = new Option(null, CLI_VALIDATION_THRESHOLD, true, "Fully qualified class name for ValidationThreshold");
    //
    public static final Option OPT_VALIDATION_FAILUREHANDLER = new Option(null, CLI_VALIDATION_FAILUREHANDLER, true, "Fully qualified class name for ValidationFailureHandler");
    //
    public static final Option OPT_SPLIT_BY = new Option(null, CLI_SPLIT_BY, true, "Column of the table used to split work units");
    //
    public static final Option OPT_SPLIT_LIMIT = new Option(null, CLI_SPLIT_LIMIT, true, "Upper Limit of rows per split for split columns of Date/Time/Timestamp and integer types. For date or timestamp fields it is calculated in seconds. split-limit should be greater than 0");
    //
    public static final Option OPT_WHERE = new Option(null, CLI_WHERE, true, "WHERE clause to use during import");
    //
    public static final Option OPT_APPEND = new Option(null, CLI_APPEND, false, "Imports data in append mode");
    //
    public static final Option OPT_DELETE = new Option(null, CLI_DELETE, true, "Imports data in delete mode");
    //
    public static final Option OPT_TARGET_DIR = new Option(null, CLI_TARGET_DIR, true, "HDFS plain table destination");
    //
    public static final Option OPT_BOUNDARY_QUERY = new Option(null, CLI_BOUNDARY_QUERY, true, "Set boundary query for retrieving max and min value of the primary key");
    //
    public static final Option OPT_MERGE_KEY = new Option(null, CLI_MERGE_KEY, true, "Key column to use to join results");
    //
    public static final Option OPT_WAREHOUSE_DIR = new Option(null, CLI_WAREHOUSE_DIR, true, "HDFS parent for table destination");
    //
    public static final Option OPT_AS_SEQUENCEFILE = new Option(null, CLI_AS_SEQUENCEFILE, false, "Imports data to SequenceFiles");
    //
    public static final Option OPT_AS_TEXTFILE = new Option(null, CLI_AS_TEXTFILE, false, "Imports data as plain text (default)");
    //
    public static final Option OPT_AS_AVRODATAFILE = new Option(null, CLI_AS_AVRODATAFILE, false, "Imports data to Avro data files");
    //
    public static final Option OPT_AS_PARQUETFILE = new Option(null, CLI_AS_PARQUETFILE, false, "Imports data to Parquet files");
    //
    public static final Option OPT_COMPRESS = new Option("z", CLI_COMPRESS, false, "Enable compression");
    //
    public static final Option OPT_COMPRESSION_CODEC = new Option(null, CLI_COMPRESSION_CODEC, true, "Compression codec to use for import");
    //
    public static final Option OPT_DIRECT_SPLIT_SIZE = new Option(null, CLI_DIRECT_SPLIT_SIZE, true, "Split the input stream every 'n' bytes when importing in direct mode");
    //
    public static final Option OPT_INLINE_LOB_LIMIT = new Option(null, CLI_INLINE_LOB_LIMIT, true, "Set the maximum size for an inline LOB");
    //
    public static final Option OPT_AUTORESET_TO_ONE_MAPPER = new Option(null, CLI_AUTORESET_TO_ONE_MAPPER, false, "Reset the number of mappers to one mapper if no split key available");

}
