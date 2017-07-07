package com.deppon.hadoop.sqoopx.core.util;

import com.deppon.hadoop.sqoopx.core.conf.ConfigurationConstants;
import com.deppon.hadoop.sqoopx.core.mapreduce.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.InputJobInfo;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;

/**
 * Created by meepai on 2017/7/4.
 */
public class HCatRecordConvertor extends Configured {

    private MapWritable colTypesJava;

    private MapWritable colTypesSql;

    private Record record;

    private InputJobInfo jobInfo;

    private HCatSchema hCatFullTableSchema;

    public HCatRecordConvertor(Configuration conf) throws IOException {
        super(conf);
        this.colTypesJava = DefaultStringifier.load(conf, ConfigurationConstants.HCAT_OUTPUT_COLTYPES_JAVA, MapWritable.class);
        this.colTypesSql = DefaultStringifier.load(conf, ConfigurationConstants.HCAT_OUTPUT_COLTYPES_SQL, MapWritable.class);
        String recordClassName = conf.get(ConfigurationConstants.SQOOPX_EXPORT_TABLE_CLASS);
        if(recordClassName == null){
            throw new IOException("Export table class name is not set!");
        }
        try {
            Class clazz = Class.forName(recordClassName);
            this.record = (Record) clazz.newInstance();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
        // 序列化的jobInfo
        String inputJobInfoStr = conf.get(HCatConstants.HCAT_KEY_JOB_INFO);
        jobInfo = (InputJobInfo)HCatUtil.deserialize(inputJobInfoStr);
        // 全表字段的Schema，需要整合数据字段和分区字段
        HCatSchema dataSchema = jobInfo.getTableInfo().getDataColumns();
        HCatSchema partitionSchema = jobInfo.getTableInfo().getPartitionColumns();
        hCatFullTableSchema = new HCatSchema(dataSchema.getFields());
        for(HCatFieldSchema fieldSchema : partitionSchema.getFields()){
            hCatFullTableSchema.append(fieldSchema);
        }
    }

    /**
     * 将HCatRecord转换成Record对象
     * @param hCatRecord
     * @return
     */
    public Record convertToRecord(HCatRecord hCatRecord) throws IOException {
        // 遍历
        Set<Map.Entry> set = record.getFieldMap().entrySet();
        for(Map.Entry entry : set){
            String colName = (String) entry.getKey();
            String lower = colName.toLowerCase();
            Object hCatVal = hCatRecord.get(lower, hCatFullTableSchema);
            String javaType = colTypesJava.get(new Text(lower)).toString();
            HCatFieldSchema fieldSchema = hCatFullTableSchema.get(lower);
            HCatFieldSchema.Type typeInfo = fieldSchema.getType();
            Object sqlVal = convertHCatVal(hCatVal, typeInfo, javaType);
            System.out.println("field: " + colName + " hCatVal: " + hCatVal + " sqlVal: " + sqlVal);
            record.setField(colName, sqlVal);
        }
        return this.record;
    }

    /**
     * @param hCatVal
     * @param typeInfo
     * @param javaType
     * @return
     */
    private Object convertHCatVal(Object hCatVal, HCatFieldSchema.Type typeInfo, String javaType){
        if(hCatVal == null){
            return null;
        }
        Object val = null;
        switch(typeInfo){
            case INT:
            case TINYINT:
            case SMALLINT:
            case FLOAT:
            case DOUBLE:
                return convertNumberTypes(hCatVal, javaType);
            case BOOLEAN:
                return convertBooleanTypes(hCatVal, javaType);
            case BIGINT:
                if(javaType.equals("java.sql.Date")){
                    return new Date((Long) hCatVal);
                } else if(javaType.equals("java.sql.Time")){
                    return new Time((Long) hCatVal);
                } else if(javaType.equals("java.sql.Timestamp")){
                    return new Timestamp((Long) hCatVal);
                } else {
                    return convertNumberTypes(val, javaType);
                }
            case DATE:
                Date date = (Date) hCatVal;
                if(javaType.equals("java.sql.Date")){
                    return date;
                } else if(javaType.equals("java.sql.Time")){
                    return new Time(date.getTime());
                } else if(javaType.equals("java.sql.Timestamp")){
                    return new Timestamp(date.getTime());
                }
                break;
            case TIMESTAMP:
                Timestamp ts = (Timestamp) hCatVal;
                if(javaType.equals("java.sql.Date")){
                    return new Date(ts.getTime());
                } else if(javaType.equals("java.sql.Time")){
                    return new Time(ts.getTime());
                } else if(javaType.equals("java.sql.Timestamp")){
                    return ts;
                }
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                return convertStringTypes(hCatVal, javaType);
            case BINARY:
                return convertBinaryTypes(hCatVal, javaType);
            case DECIMAL:
                return convertDecimalTypes(hCatVal, javaType);
            case ARRAY:
            case MAP:
            case STRUCT:
            default:
                break;
        }
        return null;
    }

    private Object convertBooleanTypes(Object val, String javaType){
        Boolean b = (Boolean) val;
        if(javaType.equals("java.lang.Boolean")){
            return b;
        } else if(javaType.equals("java.lang.Byte")){
            return (byte)(b ? 1 : 0);
        } else if(javaType.equals("java.lang.Short")){
            return (short)(b ? 1 : 0);
        } else if(javaType.equals("java.lang.Integer")){
            return (b ? 1 : 0);
        } else if(javaType.equals("java.lang.Long")){
            return (long)(b ? 1 : 0);
        } else if(javaType.equals("java.lang.Float")){
            return (float)(b ? 1 : 0);
        } else if(javaType.equals("java.lang.Double")){
            return (double)(b ? 1 : 0);
        } else if(javaType.equals("java.math.BigDecimal")){
            return new BigDecimal(b ? 1 : 0);
        } else if(javaType.equals("java.lang.String")){
            return val.toString();
        }
        return null;
    }

    private Object convertNumberTypes(Object val, String javaType){
        Number n = (Number) val;
        if(javaType.equals("java.lang.Boolean")){
            return n.byteValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        } else if(javaType.equals("java.lang.Byte")){
            return n.byteValue();
        } else if(javaType.equals("java.lang.Short")){
            return n.shortValue();
        } else if(javaType.equals("java.lang.Integer")){
            return n.intValue();
        } else if(javaType.equals("java.lang.Long")){
            return n.longValue();
        } else if(javaType.equals("java.lang.Float")){
            return n.floatValue();
        } else if(javaType.equals("java.lang.Double")){
            return n.doubleValue();
        } else if(javaType.equals("java.math.BigDecimal")){
            return new BigDecimal(n.doubleValue());
        } else if(javaType.equals("java.lang.String")){
            return val.toString();
        }
        return null;
    }

    private Object convertStringTypes(Object val, String javaType){
        String valString = val.toString();
        if(javaType.equals("java.math.BigDecimal")){
            return new BigDecimal(valString);
        } else if(javaType.equals("java.sql.Date") || javaType.equals("java.sql.Time") || javaType.equals("java.sql.Timestamp")){
            if(valString.length() == 10 && valString.matches("^\\d{4}-\\d{2}-\\d{2}$")){
                Date d = Date.valueOf(valString);
                if(javaType.equals("java.sql.Date")){
                    return d;
                } else if(javaType.equals("java.sql.Time")){
                    return new Time(d.getTime());
                } else if(javaType.equals("java.sql.Timestamp")){
                    return new Timestamp(d.getTime());
                }
            } else if(valString.length() == 8 && valString.matches("^\\d{2}:\\d{2}:\\d{2}$")){
                Time t = Time.valueOf(valString);
                if(javaType.equals("java.sql.Date")){
                    return new Date(t.getTime());
                } else if(javaType.equals("java.sql.Time")){
                    return t;
                } else if(javaType.equals("java.sql.Timestamp")){
                    return new Timestamp(t.getTime());
                }
            } else if(valString.length() >=19 && valString.length() <= 26 && valString.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}\\d{2}(.\\d+)?$")){
                Timestamp ts = Timestamp.valueOf(valString);
                if(javaType.equals("java.sql.Date")){
                    return new Date(ts.getTime());
                } else if(javaType.equals("java.sql.Time")){
                    return new Time(ts.getTime());
                } else if(javaType.equals("java.sql.Timestamp")){
                    return ts;
                }
            } else {
                return null;
            }
        } else if(javaType.equals("java.lang.String")){
            return valString;
        } else if(javaType.equals("java.lang.Boolean")){
            return Boolean.valueOf(valString);
        } else if(javaType.equals("java.lang.Byte")){
            return Byte.parseByte(valString);
        } else if(javaType.equals("java.lang.Short")){
            return Short.parseShort(valString);
        } else if(javaType.equals("java.lang.Integer")){
            return Integer.parseInt(valString);
        } else if(javaType.equals("java.lang.Long")){
            return Long.parseLong(valString);
        } else if(javaType.equals("java.lang.Float")){
            return Float.parseFloat(valString);
        } else if(javaType.equals("java.lang.Double")){
            return Double.parseDouble(valString);
        }
        return null;
    }

    private Object convertBinaryTypes(Object val, String javaType){
        byte[] bytes = (byte[]) val;
        if(javaType.equals("org.apache.hadoop.io.BytesWritable")){
            BytesWritable bw = new BytesWritable();
            bw.set(bytes, 0, bytes.length);
            return bw;
        }
        return null;
    }

    /**
     * @param val
     * @param javaType
     * @return
     */
    private Object convertDecimalTypes(Object val, String javaType){
        HiveDecimal hd = (HiveDecimal) val;
        BigDecimal bd = hd.bigDecimalValue();
        if(javaType.equals("java.math.BigDecimal")){
            return bd;
        } else if(javaType.equals("java.lang.String")){
            return bd.toPlainString();
        }
        return null;
    }

}
