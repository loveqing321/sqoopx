package com.deppon.hadoop.sqoopx.core.exception;

/**
 * 无效Options异常
 * Created by meepai on 2017/6/19.
 */
public class InvalidOptionsException extends SqoopxException {

    public InvalidOptionsException() {
    }

    public InvalidOptionsException(String message) {
        super(message);
    }

    public InvalidOptionsException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidOptionsException(Throwable cause) {
        super(cause);
    }
}
