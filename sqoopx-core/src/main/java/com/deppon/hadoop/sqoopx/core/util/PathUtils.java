package com.deppon.hadoop.sqoopx.core.util;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by meepai on 2017/6/28.
 */
public class PathUtils {

    /**
     * 获取导入路径，采用如下顺序：target-dir -> warehouse/tableName/ tableName.
     * @param options
     * @return
     */
    public static Path getImportPath(SqoopxOptions options) throws IOException {
        String warehouseDir = options.getWarehouseDir();
        String targetDir = options.getTargetDir();
        String tableName = options.getTableName();
        Path output = null;
        if(targetDir != null){
            output = new Path(targetDir);
        } else if(warehouseDir != null){
            output = new Path(warehouseDir, tableName);
        } else if(tableName != null){
            output = new Path(tableName);
        }
        if(output != null){
            return output.getFileSystem(options.getConf()).makeQualified(output);
        }
        return null;
    }

}
