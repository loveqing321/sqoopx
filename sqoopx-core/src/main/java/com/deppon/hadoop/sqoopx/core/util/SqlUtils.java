package com.deppon.hadoop.sqoopx.core.util;

import org.apache.avro.Schema;
import org.apache.hadoop.io.BytesWritable;

import java.sql.Types;

/**
 * Created by meepai on 2017/6/22.
 */
public class SqlUtils {

    /**
     * 根据sql类型获取对应的java类型
     * @param sqlType
     * @return
     */
    public static String toJavaType(int sqlType){
        if (sqlType == Types.INTEGER) {
            return "java.lang.Integer";
        } else if (sqlType == Types.VARCHAR) {
            return "java.lang.String";
        } else if (sqlType == Types.CHAR) {
            return "java.lang.String";
        } else if (sqlType == Types.LONGVARCHAR) {
            return "java.lang.String";
        } else if (sqlType == Types.NVARCHAR) {
            return "java.lang.String";
        } else if (sqlType == Types.NCHAR) {
            return "java.lang.String";
        } else if (sqlType == Types.LONGNVARCHAR) {
            return "java.lang.String";
        } else if (sqlType == Types.NUMERIC) {
            return "java.math.BigDecimal";
        } else if (sqlType == Types.DECIMAL) {
            return "java.math.BigDecimal";
        } else if (sqlType == Types.BIT) {
            return "java.lang.Boolean";
        } else if (sqlType == Types.BOOLEAN) {
            return "java.lang.Boolean";
        } else if (sqlType == Types.TINYINT) {
            return "java.lang.Integer";
        } else if (sqlType == Types.SMALLINT) {
            return "java.lang.Integer";
        } else if (sqlType == Types.BIGINT) {
            return "java.lang.Long";
        } else if (sqlType == Types.REAL) {
            return "java.lang.Float";
        } else if (sqlType == Types.FLOAT) {
            return "java.lang.Double";
        } else if (sqlType == Types.DOUBLE) {
            return "java.lang.Double";
        } else if (sqlType == Types.DATE) {
            return "java.sql.Date";
        } else if (sqlType == Types.TIME) {
            return "java.sql.Time";
        } else if (sqlType == Types.TIMESTAMP) {
            return "java.sql.Timestamp";
        } else if (sqlType == Types.BINARY
                || sqlType == Types.VARBINARY) {
            return BytesWritable.class.getName();
        } else if (sqlType == Types.CLOB) {
            return "java.lang.String";
        } else if (sqlType == Types.BLOB
                || sqlType == Types.LONGVARBINARY) {
            return "java.lang.String";
        } else {
            // TODO(aaron): Support DISTINCT, ARRAY, STRUCT, REF, JAVA_OBJECT.
            // Return null indicating database-specific manager should return a
            // java data type if it can find one for any nonstandard type.
            return null;
        }
    }

    /**
     * 根据sql类型获取hive类型
     * @param sqlType
     * @return
     */
    public static String toHiveType(int sqlType) {
        switch (sqlType) {
            case Types.INTEGER:
            case Types.SMALLINT:
                return "INT";
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.LONGNVARCHAR:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.CLOB:
                return "STRING";
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
                return "DOUBLE";
            case Types.BIT:
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.TINYINT:
                return "TINYINT";
            case Types.BIGINT:
                return "BIGINT";
            default:
                // TODO(aaron): Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT,
                // BLOB, ARRAY, STRUCT, REF, JAVA_OBJECT.
                return null;
        }
    }

    /**
     * 将指定的sql类型转换为HCat类型
     * @param sqlType
     * @return
     */
    public static String toHCatType(int sqlType) {
        switch (sqlType) {
            // Ideally TINYINT and SMALLINT should be mapped to their
            // HCat equivalents tinyint and smallint respectively
            // But the Sqoop Java type conversion has them mapped to Integer
            // Even though the referenced Java doc clearly recommends otherwise.
            // Changing this now can cause many of the sequence file usages to
            // break as value class implementations will change. So, we
            // just use the same behavior here.
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.INTEGER:
                return "int";
            case Types.VARCHAR:
                return "varchar";
            case Types.CHAR:
                return "char";
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.LONGNVARCHAR:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.CLOB:
                return "string";
            case Types.FLOAT:
            case Types.REAL:
                return "float";
            case Types.NUMERIC:
            case Types.DECIMAL:
                return "decimal";
            case Types.DOUBLE:
                return "double";
            case Types.BIT:
            case Types.BOOLEAN:
                return "boolean";
            case Types.BIGINT:
                return "bigint";
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return "binary";
            default:
                throw new IllegalArgumentException(
                        "Cannot convert SQL type to HCatalog type " + sqlType);
        }
    }

    /**
     * Resolve a database-specific type to Avro data type.
     * @param sqlType     sql type
     * @return            avro type
     */
    public static Schema.Type toAvroType(int sqlType) {
        switch (sqlType) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return Schema.Type.INT;
            case Types.BIGINT:
                return Schema.Type.LONG;
            case Types.BIT:
            case Types.BOOLEAN:
                return Schema.Type.BOOLEAN;
            case Types.REAL:
                return Schema.Type.FLOAT;
            case Types.FLOAT:
            case Types.DOUBLE:
                return Schema.Type.DOUBLE;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return Schema.Type.STRING;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
                return Schema.Type.STRING;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return Schema.Type.LONG;
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Schema.Type.BYTES;
            default:
                throw new IllegalArgumentException("Cannot convert SQL type "
                        + sqlType);
        }
    }

    /**
     * hive必须降低精度的类型
     * @param sqlType
     * @return
     */
    public static boolean isHiveTypeImprovised(int sqlType){
        return sqlType == Types.DATE || sqlType == Types.TIME || sqlType == Types.TIMESTAMP
                || sqlType == Types.DECIMAL || sqlType == Types.NUMERIC;
    }

    /**
     * Return a string identifying the character to use as a delimiter
     * in Hive, in octal representation.
     * Hive can specify delimiter characters in the form '\ooo' where
     * ooo is a three-digit octal number between 000 and 177. Values
     * may not be truncated ('\12' is wrong; '\012' is ok) nor may they
     * be zero-prefixed (e.g., '\0177' is wrong).
     *
     * @param charNum the character to use as a delimiter
     * @return a string of the form "\ooo" where ooo is an octal number
     * in [000, 177].
     * @throws IllegalArgumentException if charNum &gt; 0177.
     */
    public static String getHiveOctalCharCode(int charNum) {
        if (charNum > 0177) {
            throw new IllegalArgumentException(
                    "Character " + charNum + " is an out-of-range delimiter");
        }
        return String.format("\\%03o", charNum);
    }

}
