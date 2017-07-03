package com.deppon.hadoop.sqoopx.core;

import com.deppon.hadoop.sqoopx.core.conf.ConfigurationConstants;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by meepai on 2017/6/25.
 */
public class SqoopxMainHiveImportTest {

    public static void main(String[] args){

        List<String> list = new ArrayList<String>();
        list.add("import");
        list.add("--connect");
        list.add("jdbc:mysql://10.230.28.222:3306/sqoopx?characterEncoding=utf8&useSSL=true");
        list.add("--username");
        list.add("root");
        list.add("--password");
        list.add("111111");
        list.add("--table");
        list.add("test_word");
        list.add("--target-dir");
        list.add("/user/dp311678/output/test_hive_import");
        list.add("--hive-host");
        list.add("192.168.10.229");
        list.add("--hive-port");
        list.add("10000");
        list.add("--hive-database");
        list.add("sqoopx");
        list.add("--hive-username");
        list.add("manager");
        list.add("--hive-password");
        list.add("root123");
        list.add("--hive-import");
        list.add("--hive-table");
        list.add("test_word2");

        // 设置job id 外部系统唯一标示
        String jobId = System.currentTimeMillis() + "";
        System.setProperty(ConfigurationConstants.LAUNCHER_JOB_ID, jobId);

        SqoopxMain.main(list.toArray(new String[0]));


    }
}
