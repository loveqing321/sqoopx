package com.deppon.hadoop.sqoopx.core.util;


import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

/**
 * Created by meepai on 2017/6/27.
 */
public class FileSystemUtils {

    /**
     * make qualified
     * @param path
     * @param conf
     * @return
     * @throws IOException
     */
    public static Path makeQualified(Path path, Configuration conf) throws IOException {
        if(path == null){
            return null;
        }
        FileSystem fs = path.getFileSystem(conf);
        return fs.makeQualified(path);
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/output/t4");
        FileStatus[] statuses = fs.listStatus(path);
        Path[] paths = FileUtil.stat2Paths(statuses);
        for(Path p : paths){
            System.out.println(p);
            if(fs.isFile(p)){
                FSDataInputStream fos = fs.open(p);
                IOUtils.copy(fos, System.out);
            }
        }
    }
}
