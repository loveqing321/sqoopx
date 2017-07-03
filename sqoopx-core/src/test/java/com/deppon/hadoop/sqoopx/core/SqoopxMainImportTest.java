package com.deppon.hadoop.sqoopx.core;

import com.deppon.hadoop.sqoopx.core.conf.ConfigurationConstants;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by meepai on 2017/6/25.
 */
public class SqoopxMainImportTest {

    public static void main(String[] args){

        List<String> list = new ArrayList<String>();
        list.add("import");
        list.add("--connect");
        list.add("jdbc:mysql://localhost:3306/sqoopx?characterEncoding=utf8&useSSL=true");
        list.add("--username");
        list.add("root");
        list.add("--password");
        list.add("123456");
        list.add("--table");
        list.add("test_word");
        list.add("--import-dir");
        list.add("hdfs://127.0.0.1:9000/test/output/t3");

        // 设置job id 外部系统唯一标示
        String jobId = System.currentTimeMillis() + "";
        System.setProperty(ConfigurationConstants.LAUNCHER_JOB_ID, jobId);

        SqoopxMain.main(list.toArray(new String[0]));


    }
}
