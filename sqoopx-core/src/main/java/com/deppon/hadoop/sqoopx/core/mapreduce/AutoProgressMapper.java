package com.deppon.hadoop.sqoopx.core.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by meepai on 2017/6/21.
 */
public abstract class AutoProgressMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends SqoopxMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    private static final Logger log = Logger.getLogger(AutoProgressMapper.class);

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        ProgressThread thread = new ProgressThread(context);
        try {
            thread.setDaemon(true);
            thread.start();
            super.run(context);
        } finally {
            thread.signalShutdown();
            try {
                thread.join();
            } catch (InterruptedException e){
            }
        }
    }


    public class ProgressThread extends Thread {

//        private Context ctx;

        /**
         * 线程启动时间
         */
        private long startTimeMillis;

        /**
         * 线程上次报告时间
         */
        private long lastReportMillis;

        /**
         * 线程最大持续时间  <=0 的话永久持续
         */
        private int maxProgressPeriod;

        /**
         * 线程循环休眠时长
         */
        private int sleepInterval;

        /**
         * 进度报告最小间隔
         */
        private int reportInterval;

        /**
         * 线程循环标示
         */
        private boolean keepGoing = true;

        public ProgressThread(Context context){
//            this.ctx = context;
            Configuration configuration = context.getConfiguration();
            this.maxProgressPeriod = configuration.getInt("sqoopx.mapred.auto.progress.max", 0);
            this.sleepInterval = configuration.getInt("sqoopx.mapred.auto.progress.sleep", 10000);
            this.reportInterval = configuration.getInt("sqoopx.mapred.auto.progress.report", 30000);
            if(this.reportInterval < 1){
                log.warn("Invalid sqoopx.mapred.auto.progress.report; setting to 30000");
                this.reportInterval = 30000;
            }
            if(this.sleepInterval > this.reportInterval || this.sleepInterval < 1){
                log.warn("Invalid sqoopx.mapred.auto.progress.sleep; setting to 10000");
                this.sleepInterval = 10000;
            }
            if(this.maxProgressPeriod < 0){
                log.warn("Invalid sqoopx.mapred.auto.progress.max; setting to 0");
                this.maxProgressPeriod = 0;
            }
        }

        @Override
        public void run() {
            this.lastReportMillis = System.currentTimeMillis();
            this.startTimeMillis = this.lastReportMillis;
            long MAX_PROGRESS = this.maxProgressPeriod;
            long REPORT_INTERVAL = this.reportInterval;
            long SLEEP_INTERVAL = this.sleepInterval;

            while(this.keepGoing && !this.isInterrupted()){
                long currentMillis = System.currentTimeMillis();
                if(MAX_PROGRESS != 0L && currentMillis - this.startTimeMillis > MAX_PROGRESS){
                    this.keepGoing = false;
                    log.info("Auto-progress thread exiting after " + MAX_PROGRESS + " ms.");
                    break;
                }
                if(currentMillis - this.lastReportMillis > REPORT_INTERVAL){
                    log.debug("Auto-progress thread report progress!");
//                    this.ctx.progress();
                    this.lastReportMillis = currentMillis;
                }
                try {
                    Thread.sleep(SLEEP_INTERVAL);
                } catch (InterruptedException e) {
                }
            }
        }

        public void signalShutdown(){
            this.keepGoing = false;
            this.interrupt();
        }
    }
}
