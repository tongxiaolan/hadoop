package com.bigdata.mr.groupingcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 封装orderBean
 * @author 小懒
 * @create 2020/7/10
 * @since 1.0.0
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBean = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] filds = value.toString().split("\t");
        orderBean.setOrderId(filds[0]);
        orderBean.setProductId(filds[1]);
        orderBean.setPrice(Double.parseDouble(filds[2]));
        context.write(orderBean,NullWritable.get());
    }
}