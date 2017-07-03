package com.deppon.hadoop.sqoopx.core.mapreduce;

import com.deppon.hadoop.sqoopx.core.conf.ConfigurationConstants;
import com.deppon.hadoop.sqoopx.core.exception.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 对接到DBInputFormat的文本导出Mapper，
 * Created by meepai on 2017/6/21.
 */
public class ExportRecordMapper extends AutoProgressMapper<LongWritable, Text, Record, NullWritable> {

    private static final Logger log = Logger.getLogger(ExportRecordMapper.class);

    private Record record;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String name = context.getConfiguration().get(ConfigurationConstants.SQOOPX_EXPORT_TABLE_CLASS);
        if(name == null){
            throw new IOException("Can't find the record implement class!");
        }
        try {
            Class clazz = Class.forName(name);
            this.record = (Record) clazz.newInstance();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            this.record.parse(value);
            context.write(this.record, NullWritable.get());
        } catch (ParseException e) {
            log.error("");
            log.error("Exception raised during data mapper");
            log.error("");
            log.error("Exception: " + e);
            InputSplit split = context.getInputSplit();
            if(split instanceof FileSplit){
                log.error("On input file: " + ((FileSplit) split).getPath());
            } else if(split instanceof CombineFileSplit){
                log.error("On input file: " + context.getConfiguration().get("map.input.file"));
            }
            log.error("At position: " + key);
            log.error("");
            log.error("Currently processing split: ");
            log.error(split);
            log.error("");
            log.error("This issue might not necessarily be caused by current input");
            log.error("due to the batching nature of mapper.");
            log.error("");
            throw new IOException("Can't mapper data, please check task tracker logs", e);
        }

    }
}
