package com.deppon.hadoop.sqoopx.core.mapreduce;

import com.deppon.hadoop.sqoopx.core.util.HCatRecordConvertor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * HCatalog export mapper.
 * Created by meepai on 2017/7/4.
 */
public class HCatExportMapper extends AutoProgressMapper<WritableComparable, HCatRecord, Record, WritableComparable> {

    private static final Logger log = Logger.getLogger(HCatExportMapper.class);

    private HCatRecordConvertor convertor;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        convertor = new HCatRecordConvertor(conf);
    }

    @Override
    protected void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException {
        System.out.println("map: " + value);
        Record record = convertor.convertToRecord(value);
        System.out.println(record);
        context.write(record, NullWritable.get());
    }
}
