package com.bigdata.hbase.mr.mapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 小懒
 * @create 2020/8/1
 * @since 1.0.0
 */
public class FileToTableMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] fields = s.split(",");
        byte[] rowKey = Bytes.toBytes(fields[0]);
        Put put = new Put(rowKey);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
        context.write(new ImmutableBytesWritable(Bytes.toBytes(fields[0])),put);
    }
}