package com.deppon.hadoop.sqoopx.core.util;

import com.deppon.hadoop.sqoopx.core.options.SqoopxOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.thrift.TException;

import java.io.IOException;

/**
 * Created by meepai on 2017/6/24.
 */
public class HCatalogUtils {

    public static final String DEFAULT_DB = "default";

    /**
     * 判断table是否为视图
     * @param options
     * @return
     */
    public static boolean isHCatView(SqoopxOptions options){
        Table table = getHCatTable(options);
        return table == null ? false : table.isView();
    }

    /**
     * 获取指定的HCatTable
     * @param options
     * @return
     */
    public static Table getHCatTable(SqoopxOptions options){
        String hCatDatabaseName = getHCatDatabaseName(options);
        String hCatTableName = getHCatTableName(options);

        Configuration conf = options.getConf();
        HiveConf hiveConf;
        try {
            if(conf != null){
                hiveConf = HCatUtil.getHiveConf(conf);
            } else {
                hiveConf = new HiveConf(HCatInputFormat.class);
            }
            HiveMetaStoreClient client = HCatUtil.getHiveClient(hiveConf);
            return HCatUtil.getTable(client, hCatDatabaseName, hCatTableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取database
     * @param options
     * @return
     */
    private static String getHCatDatabaseName(SqoopxOptions options){
        String hCatDatabaseName = options.getHCatDatabaseName();
        hCatDatabaseName = hCatDatabaseName != null ? hCatDatabaseName : DEFAULT_DB;
        return hCatDatabaseName.toLowerCase();
    }

    /**
     * 获取hcattable
     * @param options
     * @return
     */
    private static String getHCatTableName(SqoopxOptions options){
        return options.getHCatTableName().toLowerCase();
    }
}
