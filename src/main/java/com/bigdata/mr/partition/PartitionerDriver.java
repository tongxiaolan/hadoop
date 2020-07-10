package com.bigdata.mr.partition;

import com.bigdata.mr.flow.FlowBean;
import com.bigdata.mr.flow.FlowMapper;
import com.bigdata.mr.flow.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 测试自定义分区驱动类
 * 其他类都用FlowBean下的
 *
 * @author 小懒
 * @create 2020/7/10
 * @since 1.0.0
 */
public class PartitionerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(PartitionerDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 设置reduce数量和partition
        job.setNumReduceTasks(5);
        job.setPartitionerClass(MyPartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}