package com.bigdata.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 自定义Hbase操作工具类
 * @author 小懒
 * @create 2020/7/31
 * @since 1.0.0
 */
public class HbaseUtil {

    /*
     * 将连接放到线程存储空间
     */
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<>();

    private HbaseUtil() {
    }

    /**
     * 创建hbase连接
     * @throws IOException
     */
    public static void mkaeHbaseConnection() throws IOException {
        Connection connection = connHolder.get();
        if (connection == null) {
            Configuration conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
            connHolder.set(connection);
        }

    }

    /**
     * 插入数据
     * @param tablename
     * @param rowKey
     * @param family
     * @param column
     * @param value
     */
    public static void insertData(String tablename,String rowKey, String family,String column,String value) throws IOException {
        Connection conn = connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();

    }



    /**
     * 关闭连接
     * @throws IOException
     */
    public static void closeHbaseConnection() throws IOException {
        Connection conn = connHolder.get();
        if (conn != null) {
            conn.close();
            connHolder.remove();
        }
    }
}