package com.deppon.hadoop.sqoopx.core.util.password;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

/**
 * 基于文件的密码加载器
 * 用于直接加载文件中的密码
 * Created by meepai on 2017/6/23.
 */
public class FilePasswordLoader implements PasswordLoader {

    /**
     * 加载密码
     * @param p
     * @param conf
     * @return
     * @throws IOException
     */
    public String loadPassword(String p, Configuration conf) throws IOException {
        Path path = new Path(p);
        FileSystem fs = path.getFileSystem(conf);
        // verify path
        verifyPath(fs, path);
        return new String(readBytes(fs, path));
    }

    /**
     * 清除敏感属性
     * @param conf
     */
    public void cleanUpSensitiveProperties(Configuration conf) {

    }

    /**
     * 验证文件路径是否合法
     * @param fs
     * @param path
     * @throws IOException
     */
    protected void verifyPath(FileSystem fs, Path path) throws IOException {
        if(!fs.exists(path)){
            throw new IOException("The provided password file does not exist!");
        }
        if(!fs.isFile(path)){
            throw new IOException("The provided password file is not a file!");
        }
    }

    /**
     * 从文件读取字节
     * @param fs
     * @param path
     * @return
     * @throws IOException
     */
    protected byte[] readBytes(FileSystem fs, Path path) throws IOException {
        InputStream in = fs.open(path);
        try {
            return IOUtils.toByteArray(in);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }
}
