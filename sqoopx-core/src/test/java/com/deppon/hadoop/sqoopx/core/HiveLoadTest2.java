package com.deppon.hadoop.sqoopx.core;

import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.PropertyConfigurator;

import java.util.Properties;

/**
 * Created by meepai on 2017/6/28.
 */
public class HiveLoadTest2 {

    public static void main(String[] args) throws Exception {
        setupLog4j();
        String[] arg = new String[]{"-f", "/Users/meepai/tmp/jars/hive-script-7482846808711962653.txt"};
        CliDriver.main(arg);

    }

    public static void setupLog4j(){
        Properties log4jProperties = new Properties();
        // root logger
        log4jProperties.setProperty("log4j.rootLogger", "INFO, A");

        // Console appender
        log4jProperties.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        log4jProperties.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.A.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");

        // mapreduce logger.
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.hive", "INFO, A");
//        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.mapreduce.Job", "INFO, job, A");
//        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.yarn.client.api.impl.YarnClientImpl", "INFO, job");

        PropertyConfigurator.configure(log4jProperties);
    }
}
