package com.deppon.hadoop.sqoopx.core.exception;

/**
 * Created by meepai on 2017/6/21.
 */
public class ParseException extends Exception {

    public ParseException() {
    }

    public ParseException(String message) {
        super(message);
    }

    public ParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParseException(Throwable cause) {
        super(cause);
    }
}
