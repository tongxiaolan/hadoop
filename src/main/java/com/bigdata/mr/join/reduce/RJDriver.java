package com.bigdata.mr.join.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * TODO  Reduce join
 * @author 小懒
 * @create 2020/7/13
 * @since 1.0.0
 */
public class RJDriver {

    /**
     * 订单数据表t_order
         id	    pid	amount
         1001	01	1
         1002	02	2
         1003	03	3
         1004	01	4
         1005	02	5
         1006	03	6

     商品信息表t_product
     pid	pname
     01	    小米
     02	    华为
     03	    格力

     要求返回数据
     id	    pname	amount
     1001	小米	    1
     1004	小米	    4
     1002	华为	    2

     思路： 先使用pid分组，使pid一致的分到一个区
     然后排序，使商品信息表t_product在第一位，然后给后续的数据赋值 pname属性
     即为： 先形成如下数据
     id     pid  amount     pname
            01	            小米
     1001	01	    1
     1004	01	    4
     然后使用第一条数据中的pname给后边的数据依次赋值。

     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(RJDriver.class);
        job.setMapperClass(RJMapper.class);
        job.setReducerClass(RJReducer.class);
        job.setGroupingComparatorClass(RJComparator.class);

        job.setOutputKeyClass(Orderbean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);

    }
}