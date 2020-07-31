package com.bigdata.hbase.testapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * filter效率低,不推荐使用
 * 如果想要增加效率,可以模拟索引,即为创建关联表,以要查询的条件为rowkey,数据为rowkey.
 * 如果有多个相同的条件,可以将其rowkey放到该条数据下不同的列中.
 * @author 小懒
 * @create 2020/7/31
 * @since 1.0.0
 */
public class TestHbaseFilter {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("my_np:student"));

        Scan scan = new Scan();
        //根据列族查
        scan.addFamily(Bytes.toBytes("info"));

        //单条件--------------------------------
        // 字节比较器
        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("2001"));
        //正则比较器  ^开始  &结束  \d数字 {几位}
        RegexStringComparator rsc = new RegexStringComparator("^\\d{3}&");

        //过滤器
        Filter rf = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, bc);
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, rsc);
        scan.setFilter(filter);
        //单条件--------------------------------

        //多条件--------------------------------
        // 必须都符合
//        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        // 符合一条
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        list.addFilter(filter);
        list.addFilter(rf);
        scan.setFilter(list);
        //多条件--------------------------------



        ResultScanner scanner = table.getScanner(scan);
        pringResult(scanner);


        table.close();
        connection.close();


    }

    private static void pringResult(ResultScanner scanner) {
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("rowkey==" + CellUtil.cloneRow(cell));
                System.out.println("family==" + CellUtil.cloneFamily(cell));
                System.out.println("column==" + CellUtil.cloneQualifier(cell));
                System.out.println("value==" + CellUtil.cloneValue(cell));
            }
        }
    }
}