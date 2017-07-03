package com.deppon.hadoop.sqoopx.core.mapreduce;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;

/**
 * Created by meepai on 2017/6/27.
 */
public class RecordWriteBridge {

    /**
     * @param val        
     * @param idx
     * @param sqlType
     * @param psmt
     * @throws SQLException
     */
    public static void writeInteger(Integer val, int idx, int sqlType, PreparedStatement psmt) throws SQLException {
        if(val == null){
            psmt.setNull(idx, sqlType);
        } else {
            psmt.setInt(idx, val);
        }
    }

    /**
     * @param val
     * @param idx
     * @param sqlType
     * @param psmt
     * @return
     * @throws SQLException
     */
    public static void writeLong(Long val, int idx, int sqlType, PreparedStatement psmt) throws SQLException {
        if(val == null){
            psmt.setNull(idx, sqlType);
        } else {
            psmt.setLong(idx, val);
        }
    }

    /**
     * @param val
     * @param idx
     * @param sqlType
     * @param psmt
     * @return
     * @throws SQLException
     */
    public static void writeFloat(Float val, int idx, int sqlType, PreparedStatement psmt) throws SQLException {
        if(val == null){
            psmt.setNull(idx, sqlType);
        } else {
            psmt.setFloat(idx, val);
        }
    }

    /**
     * @param val
     * @param idx
     * @param sqlType
     * @param psmt
     * @return
     * @throws SQLException
     */
    public static void writeDouble(Double val, int idx, int sqlType, PreparedStatement psmt) throws SQLException {
        if(val == null){
            psmt.setNull(idx, sqlType);
        } else {
            psmt.setDouble(idx, val);
        }
    }

    /**
     * @param val
     * @param idx
     * @param sqlType
     * @param psmt
     * @return
     * @throws SQLException
     */
    public static void writeBoolean(Boolean val, int idx, int sqlType, PreparedStatement psmt) throws SQLException {
        if(val == null){
            psmt.setNull(idx, sqlType);
        } else {
            psmt.setBoolean(idx, val);
        }
    }

    /**
     * @param val
     * @param idx
     * @param sqlType
     * @param psmt
     * @return
     * @throws SQLException
     */
    public static void writeString(String val, int idx, int sqlType, PreparedStatement psmt) throws SQLException {
        if(val == null){
            psmt.setNull(idx, sqlType);
        } else {
            psmt.setString(idx, val);
        }
    }

    /**
     * @param val
     * @param idx
     * @param sqlType
     * @param psmt
     * @return
     * @throws SQLException
     */
    public static void writeTime(Time val, int idx, int sqlType, PreparedStatement psmt) throws SQLException {
        if(val == null){
            psmt.setNull(idx, sqlType);
        } else {
            psmt.setTime(idx, val);
        }
    }

    /**
     * @param val
     * @param idx
     * @param sqlType
     * @param psmt
     * @return
     * @throws SQLException
     */
    public static void writeTimestamp(Timestamp val, int idx, int sqlType, PreparedStatement psmt) throws SQLException {
        if(val == null){
            psmt.setNull(idx, sqlType);
        } else {
            psmt.setTimestamp(idx, val);
        }
    }

    /**
     * @param val
     * @param idx
     * @param sqlType
     * @param psmt
     * @return
     * @throws SQLException
     */
    public static void writeDate(Date val, int idx, int sqlType, PreparedStatement psmt) throws SQLException {
        if(val == null){
            psmt.setNull(idx, sqlType);
        } else {
            psmt.setDate(idx, val);
        }
    }

    /**
     * @param val
     * @param idx
     * @param sqlType
     * @param psmt
     * @return
     * @throws SQLException
     */
    public static void writeBigDecimal(BigDecimal val, int idx, int sqlType, PreparedStatement psmt) throws SQLException {
        if(val == null){
            psmt.setNull(idx, sqlType);
        } else {
            psmt.setBigDecimal(idx, val);
        }
    }

    /**
     * @param val
     * @param idx
     * @param sqlType
     * @param psmt
     * @return
     * @throws SQLException
     */
    public static void writeBytesWritable(BytesWritable val, int idx, int sqlType, PreparedStatement psmt) throws SQLException {
        if(val == null){
            psmt.setNull(idx, sqlType);
        } else {
            byte[] rawBytes = val.getBytes();
            byte[] out = new byte[rawBytes.length];
            System.arraycopy(rawBytes, 0, out, 0, rawBytes.length);
            psmt.setBytes(idx, out);
        }
    }

    /**
     * @param val
     * @param output
     * @throws IOException
     */
    public static void writeInteger(Integer val, DataOutput output) throws IOException {
        if(val == null){
            output.writeBoolean(true);
        } else {
            output.writeBoolean(false);
            output.writeInt(val);
        }
    }

    /**
     * @param val
     * @param output
     * @throws IOException
     */
    public static void writeLong(Long val, DataOutput output) throws IOException {
        if(val == null){
            output.writeBoolean(true);
        } else {
            output.writeBoolean(false);
            output.writeLong(val);
        }
    }

    /**
     * @param val
     * @param output
     * @throws IOException
     */
    public static void writeFloat(Float val, DataOutput output) throws IOException {
        if(val == null){
            output.writeBoolean(true);
        } else {
            output.writeBoolean(false);
            output.writeFloat(val);
        }
    }

    /**
     * @param val
     * @param output
     * @throws IOException
     */
    public static void writeDouble(Double val, DataOutput output) throws IOException {
        if(val == null){
            output.writeBoolean(true);
        } else {
            output.writeBoolean(false);
            output.writeDouble(val);
        }
    }

    /**
     * @param val
     * @param output
     * @throws IOException
     */
    public static void writeBoolean(Boolean val, DataOutput output) throws IOException {
        if(val == null){
            output.writeBoolean(true);
        } else {
            output.writeBoolean(false);
            output.writeBoolean(val);
        }
    }

    /**
     * @param output
     * @throws IOException
     */
    public static void writeString(String val, DataOutput output) throws IOException {
        if(val == null){
            output.writeBoolean(true);
        } else {
            output.writeBoolean(false);
            Text.writeString(output, val);
        }
    }

    /**
     * @param output
     * @throws IOException
     */
    public static void writeTime(Time val, DataOutput output) throws IOException {
        if(val == null){
            output.writeBoolean(true);
        } else {
            output.writeBoolean(false);
            output.writeLong(val.getTime());
        }
    }

    /**
     * @param val
     * @param output
     * @throws IOException
     */
    public static void writeTimestamp(Timestamp val, DataOutput output) throws IOException {
        if(val == null){
            output.writeBoolean(true);
        } else {
            output.writeBoolean(false);
            output.writeLong(val.getTime());
        }
    }

    /**
     * @param val
     * @param output
     * @throws IOException
     */
    public static void writeDate(Date val, DataOutput output) throws IOException {
        if(val == null){
            output.writeBoolean(true);
        } else {
            output.writeBoolean(false);
            output.writeLong(val.getTime());
        }
    }

    /**
     * @param output
     * @throws IOException
     */
    public static void writeBigDecimal(BigDecimal val, DataOutput output) throws IOException {
        if(val == null){
            output.writeBoolean(true);
        } else {
            output.writeBoolean(false);
            int scale = val.scale();
            BigInteger bigInteger = val.unscaledValue();
            boolean fastPath = bigInteger.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) < 0
                    && bigInteger.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0;
            output.writeInt(scale);
            output.writeBoolean(fastPath);
            if(fastPath){
                output.writeLong(bigInteger.longValue());
            } else {
                Text.writeString(output, bigInteger.toString());
            }
        }
    }

    /**
     * @param output
     * @throws IOException
     */
    public static void writeBytesWritable(BytesWritable val, DataOutput output) throws IOException {
        if(val == null){
            output.writeBoolean(true);
        } else {
            output.writeBoolean(false);
            val.write(output);
        }
    }
}
