package com.bigdata.mr.groupingcomparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 有如下订单数据

 订单id	    商品id	成交金额
 0000001	Pdt_01	222.8
            Pdt_02	33.8
 0000002	Pdt_03	522.8
            Pdt_04	122.4
            Pdt_05	722.4
 0000003	Pdt_06	232.8
            Pdt_02	33.8
 现在需要求出每一个订单中最贵的商品
 *
 * 因为是以key输出，key的比较方法是我们自己实现的，价格低的key不会出现
 *
 * @author 小懒
 * @create 2020/7/10
 * @since 1.0.0
 */
public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(OrderDriver.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
        job.setGroupingComparatorClass(OrderComparator.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}