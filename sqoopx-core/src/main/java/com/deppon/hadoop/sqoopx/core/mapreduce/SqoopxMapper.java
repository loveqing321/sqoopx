package com.deppon.hadoop.sqoopx.core.mapreduce;

import com.deppon.hadoop.sqoopx.core.util.LoggingUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by meepai on 2017/6/21.
 */
public abstract class SqoopxMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration configuration = context.getConfiguration();
        LoggingUtils.setDebugLevel();
//        if(configuration.getBoolean("sqoopx.verbose", false)){
//            LoggingUtils.setDebugLevel();
//        }
    }
}
