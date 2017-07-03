package com.deppon.hadoop.sqoopx.core.mapreduce;

import com.deppon.hadoop.sqoopx.core.exception.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * 扩展自DBWritable和Writable
 * Created by meepai on 2017/6/21.
 */
public abstract class Record implements DBWritable, WritableComparable<Record> {

    public Record(){}

    /**
     * 从text中解析record对象
     * @param text
     * @throws ParseException
     */
    public abstract void parse(Text text) throws ParseException;

    /**
     * @param o
     * @return
     */
    public int compareTo(Record o) {
        return 0;
    }
}
