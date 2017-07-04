package com.deppon.hadoop.sqoopx.core.tools;

import com.deppon.hadoop.sqoopx.core.metadata.FilterMetadataManager;
import com.deppon.hadoop.sqoopx.core.metadata.MetadataManager;
import com.deppon.hadoop.sqoopx.core.metadata.jdbc.JdbcMetadataManagerFactory;
import com.deppon.hadoop.sqoopx.core.options.ImportOptionsConfigurer;
import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import com.deppon.hadoop.sqoopx.core.tools.job.HdfsImportJob;
import com.deppon.hadoop.sqoopx.core.tools.job.HiveImportJob;
import com.deppon.hadoop.sqoopx.core.tools.job.SqoopxJob;
import com.deppon.hadoop.sqoopx.core.tools.job.SqoopxJobContext;
import com.deppon.hadoop.sqoopx.core.util.AppendUtils;
import org.apache.hadoop.fs.Path;

/**
 * 导入工具  外部数据源到hadoop
 * Created by meepai on 2017/6/19.
 */
public class ImportTool extends BaseSqoopxTool {

    public ImportTool() {
        super(new ImportOptionsConfigurer());
    }

    public int run(SqoopxOptions options) throws Exception {
        if(!init(options)){
            return 1;
        }
        // 初始化metadataManager
        // 如果是从关系型数据库导入,目前只用户关系型数据库的导入
        MetadataManager wrapped = new JdbcMetadataManagerFactory(options).getManager();
        this.setMetadataManager(new FilterMetadataManager(wrapped));
        // 构建任务执行上下文
        SqoopxJobContext context = buildContext(options);
        SqoopxJob job;
        if(options.isHiveImport()){
            job = new HiveImportJob();
        } else {
            job = new HdfsImportJob();
        }
        job.run(context);
        return 0;
    }

    /**
     * @param options
     * @param tableName
     * @return
     */
    private Path getOutputPath(SqoopxOptions options, String tableName) {
        return getOutputPath(options, tableName, false);
    }

    /**
     * @param options
     * @param tableName
     * @param temp
     * @return
     */
    private Path getOutputPath(SqoopxOptions options, String tableName, boolean temp){
        String wareHouseDir = options.getWarehouseDir();
        String targetDir = options.getTargetDir();
        Path outputPath = null;
        if(temp){
            String salt = tableName;
            if(salt == null && options.getSqlQuery() != null){
                salt = Integer.toHexString(options.getSqlQuery().hashCode());
            }
            outputPath = AppendUtils.getTempAppendDir(salt, options);
        } else {
            if(targetDir != null){
                outputPath = new Path(targetDir);
            } else if(wareHouseDir != null){
                outputPath = new Path(wareHouseDir, tableName);
            } else if(tableName != null){
                outputPath = new Path(tableName);
            }
        }
        return outputPath;
    }
}
