package com.bigdata.mr.join.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author 小懒
 * @create 2020/7/13
 * @since 1.0.0
 */
public class RJMapper extends Mapper<LongWritable,Text,Orderbean,NullWritable> {

    private Orderbean orderbean = new Orderbean();

    //文件名称
    private String filename;

    /**
     * 获取文件名称
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        filename = fileSplit.getPath().getName();
    }

    /**
     * 数据封装
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (filename.equals("order.txt")) {
            orderbean.setId(fields[0]);
            orderbean.setPid(fields[1]);
            orderbean.setAmount(Integer.parseInt(fields[2]));
            //因为使用同一个对象来装配值，所以设置为空重置
            orderbean.setPname("");
        }else {
            orderbean.setPid(fields[0]);
            orderbean.setPname(fields[1]);
            //初始化值
            orderbean.setId("");
            orderbean.setAmount(0);
        }
        context.write(orderbean,NullWritable.get());


    }
}