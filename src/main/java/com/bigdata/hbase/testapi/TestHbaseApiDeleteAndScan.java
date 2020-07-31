package com.bigdata.hbase.testapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author 小懒
 * @create 2020/7/31
 * @since 1.0.0
 */
public class TestHbaseApiDeleteAndScan {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);


        TableName tableName = TableName.valueOf("my_np:student");
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(tableName);


        //删除表
//        dropTable(tableName, admin);
        //删除单条数据
//        deleteOne(table);
        // 删除多条数据

        // 扫描数据
        scanData(table);


    }

    /**
     * 扫描表数据
     * @param table
     * @throws IOException
     */
    private static void scanData(Table table) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
//        scan.setStartRow(Bytes.toBytes(startRoyKey))
//        scan.setStopRow(Bytes.toBytes(stopRoyKey))
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("value==" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("rowkey==" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family==" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column==" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            }
        }
    }

    /**
     * 删除单条数据
     * @param table
     * @throws IOException
     */
    private static void deleteOne(Table table) throws IOException {
        String rowKey = "1001";
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //删除某列  如果不加直接删除整条数据
        delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        table.delete(delete);
    }

    /**
     * 删除表操作
     * @param tableName
     * @param admin
     * @throws IOException
     */
    private static void dropTable(TableName tableName, Admin admin) throws IOException {
        //删除表操作
        if (admin.tableExists(tableName)) {
            //禁用表
            admin.disableTable(tableName);
            //删除表
            admin.deleteTable(tableName);
        }
    }


}