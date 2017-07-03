package com.deppon.hadoop.sqoopx.core.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by meepai on 2017/6/27.
 */
public class ImportRecordMapper extends AutoProgressMapper<LongWritable, Record, Text, NullWritable> {

    private static final Logger log = Logger.getLogger(ImportRecordMapper.class);

    @Override
    protected void map(LongWritable key, Record value, Context context) throws IOException, InterruptedException {
        context.write(new Text(value.toString()), NullWritable.get());
    }

}
