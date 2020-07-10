/**
 * Copyright (C), 2015-2020, 中发展信有限公司
 * FileName: WholeFileDriver
 * Author:   小懒
 * Date:     2020/7/9 23:31
 * Description:
 * History:
 * <author>           <version>          <desc>
 * 作者姓名            版本号              描述
 */
package com.bigdata.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * 不需要 mapper 和reducer 所以不写
 *  one two three3个为测试文件'
 *
 *  hadoop jar hdfsp-1.0-SNAPSHOT.jar com.bigdata.mr.inputformat.WholeFileDriver /textFile /textFileout
 * @author 小懒
 * @create 2020/7/9
 * @since 1.0.0
 */
public class WholeFileDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(WholeFileDriver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(WholeFIleInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }

}