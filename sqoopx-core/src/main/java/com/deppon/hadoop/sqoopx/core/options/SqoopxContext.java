package com.deppon.hadoop.sqoopx.core.options;

/**
 * Sqoopx执行上下文环境
 * Created by meepai on 2017/6/22.
 */
public class SqoopxContext {

    /**
     * 操作方法 导入／导出等。
     */
    public Operation operation;

    /**
     * 集群内部源  如果操作为导入，那么inner作为目标源，outer作为数据源
     */
    public InnerSource inner;

    /**
     * 集群外部源 如果操作为导入，那么outer作为目标源，inner作为数据源
     */
    public OuterSource outer;

    /**
     * 操作类型
     */
    enum Operation {
        IMPORT,
        EXPORT,
    }

    /**
     * 集群内部源
     */
    enum InnerSource {
        HIVE,
        AVRO,
        TEXT,
        HBASE,
    }

    enum OuterSource {
        FILE,
        REDIS,
        DB,
    }

}
