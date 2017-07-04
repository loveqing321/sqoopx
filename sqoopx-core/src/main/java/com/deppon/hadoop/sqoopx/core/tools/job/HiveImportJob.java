package com.deppon.hadoop.sqoopx.core.tools.job;

import com.deppon.hadoop.sqoopx.core.file.FileLayout;
import com.deppon.hadoop.sqoopx.core.hive.HiveScriptBuilder;
import com.deppon.hadoop.sqoopx.core.metadata.jdbc.HiveJdbcConnManager;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.SQLException;

/**
 * Created by meepai on 2017/6/26.
 */
public class HiveImportJob extends HdfsImportJob {

    private static final Logger log = Logger.getLogger(HiveImportJob.class);

    public HiveImportJob(){
        super("sqoopx-hive-import-");
    }
    @Override
    protected void doRun(SqoopxJobContext context) throws Exception {
        super.doRun(context);
        SqoopxOptions options = context.getOptions();
        // 然后将数据load到hive表中
        if(options.isHiveImport()){
            if(options.getFileLayout() != FileLayout.ParquetFile){
                loadToHive(options);
            }
        }
    }

    /**
     * @param options
     * @throws IOException
     */
    private void loadToHive(SqoopxOptions options) throws IOException {
        String inputTable = options.getTableName();
        String outputTable = options.getHiveTableName();
        if(outputTable == null){
            outputTable = inputTable;
        }
        HiveScriptBuilder builder = new HiveScriptBuilder(options, metadataManager, inputTable, outputTable, options.getConf());
        try {
            String createTableStr = builder.getCreateTableStmt();
            String loadDataStr = builder.getLoadDataStmt();
            Path finalPath = builder.getFinalPath();
            // 如果是mapreduce生成的目录，需要先移除logs文件
            this.removeTempLogs(finalPath, options);
            String codec = options.getCompressionCodes();
            if(codec != null && (codec.equals("lzop") || codec.equals("com.hadoop.compression.lzo.LzopCodec"))){
                // TODO
            }
            executeHiveSql(options, createTableStr, loadDataStr);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
        }
    }

    /**
     * 移除mapreduce产生的日志目录
     * @param path
     * @param options
     * @throws IOException
     */
    private void removeTempLogs(Path path, SqoopxOptions options) throws IOException {
        FileSystem fs = path.getFileSystem(options.getConf());
        Path logs = new Path(path, "_logs");
        if(fs.exists(logs)){
            if(!fs.delete(logs, true)){
                log.warn("Cannot delete temporary logs file; continuing with import, but it may fail.");
            }
        }
    }

    /**
     * 执行hive sql
     * @param options
     * @param sqls
     * @throws SQLException
     */
    private void executeHiveSql(SqoopxOptions options, String... sqls) throws SQLException {
        HiveJdbcConnManager hiveConnManager = new HiveJdbcConnManager(options);
        for(int i=0; i<sqls.length; i++){
            System.out.println(sqls[i]);
            hiveConnManager.execute(sqls[i]);
        }
        hiveConnManager.close();
    }

    /**
     * @param dir
     * @param createTableStr
     * @param loadDataStr
     * @return
     * @throws IOException
     */
    private File createScriptFile(String dir, String createTableStr, String loadDataStr) throws IOException {
        File file = null;
        BufferedWriter bw = null;
        try {
            file = File.createTempFile("hive-script-", ".txt", new File(dir));
            FileOutputStream fos = new FileOutputStream(file);
            bw = new BufferedWriter(new OutputStreamWriter(fos));
            bw.write(createTableStr, 0, createTableStr.length());
            bw.write(loadDataStr, 0, loadDataStr.length());
        } finally {
            IOUtils.closeQuietly(bw);
        }
        return file;
    }

    /**
     * 执行脚本
     * @param scriptFile
     * @throws Exception
     */
    private void executeScript(String scriptFile) throws Exception {
        String[] args = new String[]{"-f", scriptFile};
        CliDriver.main(args);
    }

    @Override
    public void cleanUp() throws IOException {
        super.cleanUp();
        if(fs.exists(destination)){
            fs.delete(destination, true);
        }
    }
}