package com.deppon.hadoop.sqoopx.core.mapreduce;

/**
 * Created by meepai on 2017/6/20.
 */
public class DelimiterSet {

    public static final char NULL_CHAR = '\000';

    public static final DelimiterSet DEFAULT_DELIMITERS = new DelimiterSet(',', '\n', '\u0000', '\u0000', false);

    public static final DelimiterSet HIVE_DELIMITERS = new DelimiterSet('\u0001', '\n', '\u0000', '\u0000', false);

    public static final DelimiterSet MYSQL_DELIMITERS = new DelimiterSet(',', '\n', '\'', '\\', false);

    private char fieldDelim;

    private char recordDelim;

    private char enclosedBy;

    private char escapedBy;

    private boolean encloseRequired;

    public DelimiterSet(){
        this(',', '\n', NULL_CHAR, NULL_CHAR, false);
    }

    public DelimiterSet(char fieldDelim, char recordDelim, char enclosedBy, char escapedBy, boolean encloseRequired) {
        this.fieldDelim = fieldDelim;
        this.recordDelim = recordDelim;
        this.enclosedBy = enclosedBy;
        this.escapedBy = escapedBy;
        this.encloseRequired = encloseRequired;
    }

    public char getFieldDelim() {
        return fieldDelim;
    }

    public void setFieldDelim(char fieldDelim) {
        this.fieldDelim = fieldDelim;
    }

    public char getRecordDelim() {
        return recordDelim;
    }

    public void setRecordDelim(char recordDelim) {
        this.recordDelim = recordDelim;
    }

    public char getEnclosedBy() {
        return enclosedBy;
    }

    public void setEnclosedBy(char enclosedBy) {
        this.enclosedBy = enclosedBy;
    }

    public char getEscapedBy() {
        return escapedBy;
    }

    public void setEscapedBy(char escapedBy) {
        this.escapedBy = escapedBy;
    }

    public boolean isEncloseRequired() {
        return encloseRequired;
    }

    public void setEncloseRequired(boolean encloseRequired) {
        this.encloseRequired = encloseRequired;
    }
}
