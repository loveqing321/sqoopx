package com.deppon.hadoop.sqoopx.core.util;

import com.google.common.base.Preconditions;
import org.apache.hadoop.util.Shell;

/**
 * Created by meepai on 2017/6/19.
 */
public class FileUtils {

    /**
     * 解决windows环境路径可能包含\"
     * @param path
     * @return
     */
    public static String makeQualified(String path){
        Preconditions.checkArgument(path != null);
        if(Shell.WINDOWS){
            if(path.length() > 0 && path.indexOf(0) == '"'){
                path = path.substring(1);
            }
            if(path.length() > 0 && path.indexOf(path.length()-1) == '"'){
                path = path.substring(0, path.length()-1);
            }
        }
        return path;
    }

    public static void main(String[] args){
        String s = "\"sss\"\"";
        System.out.println(FileUtils.makeQualified(s));
    }
}
