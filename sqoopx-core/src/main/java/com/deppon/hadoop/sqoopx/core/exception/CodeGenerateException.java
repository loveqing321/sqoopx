package com.deppon.hadoop.sqoopx.core.exception;

/**
 * Created by meepai on 2017/6/25.
 */
public class CodeGenerateException extends SqoopxException {

    public CodeGenerateException() {
    }

    public CodeGenerateException(String message) {
        super(message);
    }

    public CodeGenerateException(String message, Throwable cause) {
        super(message, cause);
    }

    public CodeGenerateException(Throwable cause) {
        super(cause);
    }
}
