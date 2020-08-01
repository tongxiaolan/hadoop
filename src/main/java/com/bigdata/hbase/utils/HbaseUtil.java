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
     * 生成分区键
     * @param regionCount
     * @return
     */
    public static byte[][] getRegionKeys(int regionCount) {
        byte[][] regionKeys = new byte[regionCount - 1][];
        // 3==>2-1  生成分区键 0,1
        for (int i = 0; i < regionCount - 1; i++) {
            regionKeys[i] = Bytes.toBytes(i + "|");
        }
        return regionKeys;
    }


    /**
     * 根据分区生成带有分区的rowKey
     * @param rowKey
     * @param regionCount 分区数
     * @return
     */
    public static  String getRegionNum(String rowKey,int regionCount) {
        int regionNum;
        int hashCode = rowKey.hashCode();
        // 2的n次方& (2的n次方-1) = 0
        if (regionCount > 0 && (regionCount & (regionCount - 1)) == 0) {
            // regionCount为2的n次方
            regionNum = hashCode & (regionCount - 1);
        }else {
            //如果不是2的n次方，那么hash取余
            regionNum = hashCode % (regionCount);
        }
        return regionNum + "_" + rowKey;
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