package com.deppon.hadoop.sqoopx.core.ext;

import com.deppon.hadoop.sqoopx.core.Sqoopx;
import com.deppon.hadoop.sqoopx.core.exception.SqoopxException;
import org.apache.log4j.Logger;

/**
 * Created by meepai on 2017/6/20.
 */
public class LogSqoopxExtension implements SqoopxExtension {

    private static final Logger log = Logger.getLogger(LogSqoopxExtension.class);

    public void doPreSqoopx(Sqoopx sqoopx) throws SqoopxException {
        log.info("sqoopx job [" + sqoopx.getTool().getToolName() + "] started at time: " + System.currentTimeMillis());
    }

    public void doPostSqoopx(Sqoopx sqoopx) throws SqoopxException {
        log.info("sqoopx job [" + sqoopx.getTool().getToolName() + "] stopped at time: " + System.currentTimeMillis());
    }
}
