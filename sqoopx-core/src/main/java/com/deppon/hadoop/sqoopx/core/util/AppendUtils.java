package com.deppon.hadoop.sqoopx.core.util;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import org.apache.hadoop.fs.Path;

import java.util.UUID;

/**
 * Created by meepai on 2017/6/26.
 */
public class AppendUtils {


    /**
     * @param salt
     * @param options
     * @return
     */
    public static Path getTempAppendDir(String salt, SqoopxOptions options){
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        String tempDir = options.getTempRootDir() + Path.SEPARATOR + uuid + "_" + salt;
        return new Path(tempDir);
    }
}
