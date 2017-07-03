package com.deppon.hadoop.sqoopx.core;

import com.deppon.hadoop.sqoopx.core.conf.ConfigurationConstants;
import com.deppon.hadoop.sqoopx.core.tools.SqoopxTool;
import com.deppon.hadoop.sqoopx.core.util.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by meepai on 2017/6/19.
 */
public class SqoopxMain {

    private static final Logger log = Logger.getLogger(SqoopxMain.class);

    private static Pattern[] JOB_ID_PTNS = {
            Pattern.compile("Job complete: (job_\\S*)"),
            Pattern.compile("Job (job_\\S*) has completed successfully"),
            Pattern.compile("Submitted application (application[0-9_]*)")
    };

    public static void main(String[] args){
        System.setProperty("HADOOP_USER_NAME", "manager");
        // 配置sqoopx-site配置文件
        Configuration conf = setupSqoopxSite();
        // 添加其他配置
        // 配置log4j配置，保证本次执行job的日志输出
        String logFile = setupSqoopxLog4j(conf);
        // 查杀之前的调度任务
//        killChildYarnJobs(conf);

        if(args.length <= 0){
            log.error("args length must great then zero!");
            System.exit(1);
        }
        SqoopxTool tool = SqoopxTool.getTool(args[0]);
        if(tool == null){
            throw new RuntimeException("Unknown command " + args[0] + ", accepted are export、import etc.");
        }
        Sqoopx sqoopx = new Sqoopx(tool, conf);
        try {
            ToolRunner.run(sqoopx, Arrays.copyOfRange(args, 1, args.length));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            String jobIds = extractJobIds(logFile, JOB_ID_PTNS);
            System.out.println("extract jobId from logFile: " + logFile + ", jobIds: " + jobIds);
        }
    }

    /**
     * 设置加载sqoopx-site.xml配置文件
     * @return
     */
    public static Configuration setupSqoopxSite(){
        Configuration conf = new Configuration();
        String site = System.getProperty("sqoopx.conf.xml");
        InputStream in = null;
        if(site != null && new File(FileUtils.makeQualified(site)).exists()){
            try {
                in = new FileInputStream(new File(site));
            } catch (FileNotFoundException e) {
            }
        } else {
            in = Thread.currentThread().getContextClassLoader().getResourceAsStream("sqoopx-site.xml");
        }
        // 如果连默认配置都没有找到的话，那么报错！
        if(in == null){
            throw new RuntimeException("sqoopx configuration file is not exists!");
        }
        conf.addResource(in);
        return conf;
    }

    /**
     * 配置Sqoopx对应的log4j配置
     * 主要用于捕捉hadoop mapreduce任务相关的日志到日志文件，方便后续根据日志文件来查杀任务！
     */
    public static String setupSqoopxLog4j(Configuration configuration){
        String jobId = System.getProperty(ConfigurationConstants.LAUNCHER_JOB_ID);
        if(jobId == null){
            throw new RuntimeException("miss job id in the system env property!");
        }
        String jobLogsDir = configuration.get(ConfigurationConstants.LAUNCHER_JOB_LOGS_DIR, "logs/");
        String logFile = new File(jobLogsDir + "sqoopx-" + jobId + ".log").getAbsolutePath();

        Properties log4jProperties = new Properties();
        // root logger
        log4jProperties.setProperty("log4j.rootLogger", "INFO, A");

        // Console appender
        log4jProperties.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        log4jProperties.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.A.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");

        // log file appender
        log4jProperties.setProperty("log4j.appender.job", "org.apache.log4j.FileAppender");
        log4jProperties.setProperty("log4j.appender.job.file", logFile);
        log4jProperties.setProperty("log4j.appender.job.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.job.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");

        // mapreduce logger.
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.mapred", "INFO, job, A");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.mapreduce.Job", "INFO, job, A");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.yarn.client.api.impl.YarnClientImpl", "INFO, job");

        PropertyConfigurator.configure(log4jProperties);

        return logFile;
    }

    /**
     * 查杀之前的调度任务，防止重复调度
     * @param configuration
     */
    public static void killChildYarnJobs(Configuration configuration){
        List<ApplicationId> applicationIds = getChildUnFinishedJobs(configuration);
        if(applicationIds != null && applicationIds.size() > 0){
            YarnClient yarnClient = YarnClient.createYarnClient();
            yarnClient.init(configuration);
            yarnClient.start();
            for(int i=0; i<applicationIds.size(); i++){
                ApplicationId applicationId = applicationIds.get(i);
                try {
                    yarnClient.killApplication(applicationId);
                } catch (YarnException e) {
                    throw new RuntimeException("error occurred when killing the yarn job!", e);
                } catch (IOException e) {
                    throw new RuntimeException("error occurred when killing the yarn job!", e);
                }
            }
        }
    }

    /**
     * 获取子任务ID
     * @param configuration
     * @return
     */
    private static List<ApplicationId> getChildUnFinishedJobs(Configuration configuration){
        String tag = System.getProperty(ConfigurationConstants.LAUNCHER_CHILD_JOB_TAG);
        if(tag == null){
            throw new RuntimeException("must special the child job's tag!");
        }
        List<ApplicationId> applicationIds = new ArrayList<ApplicationId>();
        GetApplicationsRequest gar = GetApplicationsRequest.newInstance();
        gar.setScope(ApplicationsRequestScope.OWN);
        gar.setApplicationTags(Collections.singleton(tag));
        gar.setStartRange(1l, System.currentTimeMillis());

        try {
            ApplicationClientProtocol proxy = ClientRMProxy.createRMProxy(configuration, ApplicationClientProtocol.class);
            GetApplicationsResponse apps = proxy.getApplications(gar);
            List<ApplicationReport> appliationList = apps.getApplicationList();
            if(appliationList != null){
                for(int i=0; i<appliationList.size(); i++){
                    // 非结束状态的任务
                    if(appliationList.get(i).getYarnApplicationState().ordinal() < YarnApplicationState.FINISHED.ordinal()){
                        applicationIds.add(appliationList.get(i).getApplicationId());
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("request application fail!", e);
        } catch (YarnException e) {
            throw new RuntimeException("request application fail!", e);
        }
        return applicationIds;
    }

    /**
     * @param logFile
     * @param patterns
     * @return
     */
    public static String extractJobIds(String logFile, Pattern[] patterns){
        Set<String> jobIds = new HashSet<String>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(logFile));
            String line = null;
            while((line = br.readLine()) != null){
                for(Pattern ptn : patterns){
                    Matcher matcher = ptn.matcher(line);
                    if(matcher.find()){
                        String jobId = matcher.group(1);
                        if(jobId.isEmpty() || jobId.equals("NULL")){
                            continue;
                        }
                        jobId = jobId.replaceAll("application", "job");
                        jobIds.add(jobId);
                    }
                }
            }
            return jobIds.isEmpty() ? null : StringUtils.join(jobIds, ",");
        } catch (FileNotFoundException e) {
            log.error("log file has not found!");
        } catch (IOException e) {
            log.error("exception occurred while reading the log file!", e);
        } finally {
            if(br != null){
                try {
                    br.close();
                } catch (IOException e) {
                }
            }
        }
        return null;
    }

}
