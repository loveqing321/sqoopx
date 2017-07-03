package com.deppon.hadoop.sqoopx.core.ext;

import com.deppon.hadoop.sqoopx.core.Sqoopx;
import com.deppon.hadoop.sqoopx.core.exception.SqoopxException;

/**
 * Created by meepai on 2017/6/19.
 */
public interface SqoopxExtension {

    /**
     * 在sqoopx执行之前
     */
    void doPreSqoopx(Sqoopx sqoopx) throws SqoopxException;

    /**
     * 在sqoopx执行之后
     * @param sqoopx
     */
    void doPostSqoopx(Sqoopx sqoopx) throws SqoopxException;

    /**
     * 默认的前置sqoopx扩展
     */
    abstract class PreSqoopxExtension implements SqoopxExtension {

        public void doPostSqoopx(Sqoopx sqoopx) throws SqoopxException {
            // do nothing.
        }
    }

    /**
     * 默认的后置sqoop扩展
     */
    abstract class PostSqoopxExtension implements SqoopxExtension {

        public void doPreSqoopx(Sqoopx sqoopx) throws SqoopxException {
            // do nothing.
        }
    }
}
