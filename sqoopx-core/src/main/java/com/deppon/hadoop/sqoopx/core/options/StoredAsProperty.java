package com.deppon.hadoop.sqoopx.core.options;

import java.lang.annotation.*;

/**
 * Created by meepai on 2017/6/26.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface StoredAsProperty {

    String value();
}
