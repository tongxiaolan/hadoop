package com.bigdata.hbase.testapi;

import com.bigdata.hbase.utils.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 创建表，并预分区（3个分区）
 *  注意 rowkey增加分区以后，查询也要加分区号来查
 * @author 小懒
 * @create 2020/8/1
 * @since 1.0.0
 */
public class TestHbaseCreateTableRegion {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        // 建表
        createRetionTable(connection);


        // 插入数据
        Table table = connection.getTable(TableName.valueOf("my_np:emp"));
        //将rowkey均匀分配到不同的分区中。 生成分区rowKey
        String rowKey = HbaseUtil.getRegionNum("zhangsan",3);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(20));
        table.put(put);
        table.close();

        connection.close();
    }

    /**
     * 创建分区表
     * @param connection
     * @throws IOException
     */
    private static void createRetionTable(Connection connection) throws IOException {
        // 获取操作对象并创建表描述对象
        Admin admin = connection.getAdmin();
        HTableDescriptor td = new HTableDescriptor(TableName.valueOf("my_np:emp"));

        //3个区，所以2个分区键
        byte[][] bs = new byte[2][];
        /*
         *  '|'在ASCII码中是第二大的值(第一大是'}')，
         *   所以只要以0开头，就会在0分区，只要以1开头，就会在1分区
         */
        bs[0] = Bytes.toBytes("0|");
        bs[1] = Bytes.toBytes("1|");

        // 使用工具类生成
//        bs = HbaseUtil.getRegionKeys(3);
        admin.createTable(td,bs);
        admin.close();
    }

}