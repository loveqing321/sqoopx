package com.deppon.hadoop.sqoopx.core.mapreduce;

import com.deppon.hadoop.sqoopx.core.exception.ParseException;
import com.deppon.hadoop.sqoopx.core.util.IdentifierUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

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
     * 生成类必须实现该方法，用于与hcatalog table的schame对用
     * 映射关系为列名与值的映射关系
     * @return
     */
    public abstract Map getFieldMap();

    /**
     * 设置字段值 根据列名来设置字段值
     * @param col
     * @param value
     */
    public void setField(String col, Object value){
        try {
            String field = IdentifierUtils.toJavaIdentifier(col);
            Field f = this.getClass().getDeclaredField(field);
            f.setAccessible(true);
            f.set(this, value);
        } catch (NoSuchFieldException e) {
        } catch (IllegalAccessException e) {
        }
    }

    /**
     * @param o
     * @return
     */
    public int compareTo(Record o) {
        return 0;
    }
}
