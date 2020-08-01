package com.bigdata.hbase.coprocesser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**协处理器 测试类 重新建表
 * @author 小懒
 * @create 2020/8/1
 * @since 1.0.0
 */
public class InsertMyNpStudent {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        TableName student = TableName.valueOf("student");
        boolean studentFlag = admin.tableExists(student);
        if (studentFlag) {
            // 已存在 删除表
            //禁用表
            admin.disableTable(student);
            //删除表
            admin.deleteTable(student);
        }


        HTableDescriptor td = new HTableDescriptor(TableName.valueOf("student"));
        //增加协处理器
        td.addCoprocessor("com.bigdata.hbase.coprocesser.InsertMyNpStudentCoprocesser");
        //增加列族 addFamily调用多次即可增加多个列族
        td.addFamily(new HColumnDescriptor("info"));
        // 创建表
        admin.createTable(td);


    }
}