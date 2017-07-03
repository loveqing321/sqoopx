package com.deppon.hadoop.sqoopx.core.exception;

/**
 * Created by meepai on 2017/6/19.
 */
public class SqoopxException extends Exception {

    public SqoopxException() {
    }

    public SqoopxException(String message) {
        super(message);
    }

    public SqoopxException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqoopxException(Throwable cause) {
        super(cause);
    }
}
