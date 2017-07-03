package com.deppon.hadoop.sqoopx.core.mapreduce;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;

/**
 * Created by meepai on 2017/6/27.
 */
public class RecordReadBridge {

    /**
     * @param col
     * @param rs
     * @return
     * @throws SQLException
     */
    public static Integer readInteger(int col, ResultSet rs) throws SQLException {
        int val = rs.getInt(col);
        if(rs.wasNull()){
            return null;
        } else {
            return Integer.valueOf(val);
        }
    }

    /**
     * @param col
     * @param rs
     * @return
     * @throws SQLException
     */
    public static Long readLong(int col, ResultSet rs) throws SQLException {
        long val = rs.getLong(col);
        if(rs.wasNull()){
            return null;
        } else {
            return Long.valueOf(val);
        }
    }

    /**
     * @param col
     * @param rs
     * @return
     * @throws SQLException
     */
    public static Float readFloat(int col, ResultSet rs) throws SQLException {
        float val = rs.getFloat(col);
        if(rs.wasNull()){
            return null;
        } else {
            return Float.valueOf(val);
        }
    }

    /**
     * @param col
     * @param rs
     * @return
     * @throws SQLException
     */
    public static Double readDouble(int col, ResultSet rs) throws SQLException {
        double val = rs.getDouble(col);
        if(rs.wasNull()){
            return null;
        } else {
            return Double.valueOf(val);
        }
    }

    /**
     * @param col
     * @param rs
     * @return
     * @throws SQLException
     */
    public static Boolean readBoolean(int col, ResultSet rs) throws SQLException {
        boolean val = rs.getBoolean(col);
        if(rs.wasNull()){
            return null;
        } else {
            return Boolean.valueOf(val);
        }
    }

    /**
     * @param col
     * @param rs
     * @return
     * @throws SQLException
     */
    public static String readString(int col, ResultSet rs) throws SQLException {
        return rs.getString(col);
    }

    /**
     * @param col
     * @param rs
     * @return
     * @throws SQLException
     */
    public static Time readTime(int col, ResultSet rs) throws SQLException {
        return rs.getTime(col);
    }

    /**
     * @param col
     * @param rs
     * @return
     * @throws SQLException
     */
    public static Timestamp readTimestamp(int col, ResultSet rs) throws SQLException {
        return rs.getTimestamp(col);
    }

    /**
     * @param col
     * @param rs
     * @return
     * @throws SQLException
     */
    public static Date readDate(int col, ResultSet rs) throws SQLException {
        return rs.getDate(col);
    }

    /**
     * @param col
     * @param rs
     * @return
     * @throws SQLException
     */
    public static BigDecimal readBigDecimal(int col, ResultSet rs) throws SQLException {
        return rs.getBigDecimal(col);
    }

    /**
     * @param col
     * @param rs
     * @return
     * @throws SQLException
     */
    public static BytesWritable readBytesWritable(int col, ResultSet rs) throws SQLException {
        byte[] bytes = rs.getBytes(col);
        return bytes == null ? null : new BytesWritable(bytes);
    }

    /**
     * @param input
     * @return
     * @throws SQLException
     */
    public static Integer readInteger(DataInput input) throws IOException {
        if(input.readBoolean()){
            return null;
        }
        return Integer.valueOf(input.readInt());
    }

    /**
     * @param input
     * @return
     * @throws IOException
     */
    public static Long readLong(DataInput input) throws IOException {
        if(input.readBoolean()){
            return null;
        }
        return Long.valueOf(input.readLong());
    }

    /**
     * @param input
     * @return
     * @throws SQLException
     */
    public static Float readFloat(DataInput input) throws IOException {
        if(input.readBoolean()){
            return null;
        }
        return Float.valueOf(input.readFloat());
    }

    /**
     * @param input
     * @return
     * @throws IOException
     */
    public static Double readDouble(DataInput input) throws IOException {
        if(input.readBoolean()){
            return null;
        }
        return Double.valueOf(input.readDouble());
    }

    /**
     * @param input
     * @return
     * @throws IOException
     */
    public static Boolean readBoolean(DataInput input) throws IOException {
        if(input.readBoolean()){
            return null;
        }
        return Boolean.valueOf(input.readBoolean());
    }

    /**
     * @param input
     * @return
     * @throws IOException
     */
    public static String readString(DataInput input) throws IOException {
        if(input.readBoolean()){
            return null;
        }
        return Text.readString(input);
    }

    /**
     * @param input
     * @return
     * @throws IOException
     */
    public static Time readTime(DataInput input) throws IOException {
        if(input.readBoolean()){
            return null;
        }
        return new Time(input.readLong());
    }

    /**
     * @param input
     * @return
     * @throws IOException
     */
    public static Timestamp readTimestamp(DataInput input) throws IOException {
        if(input.readBoolean()){
            return null;
        }
        return new Timestamp(input.readLong());
    }

    /**
     * @param input
     * @return
     * @throws IOException
     */
    public static Date readDate(DataInput input) throws IOException {
        if(input.readBoolean()){
            return null;
        }
        return new Date(input.readLong());
    }

    /**
     * @param input
     * @return
     * @throws IOException
     */
    public static BigDecimal readBigDecimal(DataInput input) throws IOException {
        if(input.readBoolean()){
            return null;
        }
        int scale = input.readInt();
        boolean fastPath = input.readBoolean();
        BigInteger unscaled;
        if(fastPath){
            long unscaledValue = input.readLong();
            unscaled = BigInteger.valueOf(unscaledValue);
        } else {
            String unscaledValueStr = Text.readString(input);
            unscaled = new BigInteger(unscaledValueStr);
        }
        return new BigDecimal(unscaled, scale);
    }

    /**
     * @param input
     * @return
     * @throws IOException
     */
    public static BytesWritable readBytesWritable(DataInput input) throws IOException {
        if(input.readBoolean()){
            return null;
        }
        BytesWritable bytesWritable = new BytesWritable();
        bytesWritable.readFields(input);
        return bytesWritable;
    }

}
