package com.bigdata.hbase.mr.mapper;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * TableMapper<rowkey,result>
 * @author 小懒
 * @create 2020/7/31
 * @since 1.0.0
 */
public class ScanDataMapper extends TableMapper<ImmutableBytesWritable,Put>{

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        // 运行mapper 查询数据

        // scan result => put
        Put put = new Put(key.get());
        //把每一列加入到put中
        for (Cell cell : result.rawCells()) {
            put.addColumn(
                    CellUtil.cloneFamily(cell),
                    CellUtil.cloneQualifier(cell),
                    CellUtil.cloneValue(cell)
            );
        }
        context.write(key,put);

    }
}