/**
 * Copyright (C), 2015-2020, 中发展信有限公司
 * FileName: FlowDriver
 * Author:   小懒
 * Date:     2020/7/9 20:07
 * Description:
 * History:
 * <author>           <version>          <desc>
 * 作者姓名            版本号              描述
 */
package com.bigdata.mr.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义Writable <br>
 * 处理数据： flow_data.log
 * 序号  手机号       ip                访问网站        上行    下行    状态码
 * 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
 * 2	13846544121	192.196.100.2			264	0	200
 * 3 	13956435636	192.196.100.3			132	1512	200
 * <p>
 * 要求：统计每个手机号的上下行溜了和总流量
 *
 * @author 小懒
 * @create 2020/7/9
 * @since 1.0.0
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);


    }

}