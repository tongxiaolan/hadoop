package com.bigdata.hbase.testapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * hbase api测试
 * 创建命名空间，表，判断表是否存在
 * 查询数据(单条)，插入数据
 * @author 小懒
 * @create 2020/7/31
 * @since 1.0.0
 */
public class TestHbaseApiSelectOneAndCreate {

    public static void main(String[] args) throws IOException {
        // 1 创建conf
        Configuration conf = HBaseConfiguration.create();
        // 2 创建连接  获取连接对象
        // classLoader HBaseConfiguration.class.getClassLoader()
        //加载配置文件 hbase-common包下：hbase-default.xml  classpath:hbase-site.xml
        Connection connection = ConnectionFactory.createConnection(conf);
//        System.out.println(connection);

        // 3 获取操作对象 Admin  主要管理表以及命名空间 DDL
        // 获取方式1  过时
//        new HBaseAdmin(conf);
        Admin admin = connection.getAdmin();

        // 4 操作数据库

        // 4-1 判断命名空间  getNamespaceDescriptor获取不到直接抛异常
        try {
            NamespaceDescriptor my_np = admin.getNamespaceDescriptor("my_np");
        }catch (NamespaceNotFoundException e) {
            // 创建命名空间
            admin.createNamespace(NamespaceDescriptor.create("my_np").build());
        }

        // 4-2 判断表是否存在
        TableName student = TableName.valueOf("my_np:student");
        boolean studentFlag = admin.tableExists(student);
        if (studentFlag) {
            //  查询数据
            // 获取表对象
            Table table = connection.getTable(student);

            //查询数据
            Get get = new Get(Bytes.toBytes("1001"));
            Result result = table.get(get);
            boolean empty = result.isEmpty();
            //如果是空的没有数据
            if(empty) {
                Put put = new Put(Bytes.toBytes("1001"));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
                table.put(put);
            }else {
                for (Cell cell : result.rawCells()) {
                    System.out.println("value==" + Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println("rowkey==" + Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println("family==" + Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println("column==" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                }
            }


        }else {
            //  创建表
            //创建表描述对象
            HTableDescriptor td = new HTableDescriptor(student);
            //增加列族 addFamily调用多次即可增加多个列族
            td.addFamily(new HColumnDescriptor("info"));
            // 创建表
            admin.createTable(td);
        }
    }
}