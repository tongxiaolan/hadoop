package com.bigdata.mr.ouputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 小懒
 * @create 2020/7/13
 * @since 1.0.0
 */
public class MyRecordWriter extends RecordWriter<LongWritable,Text>{


    private FSDataOutputStream baiduStream;
    private FSDataOutputStream otherStream;

    public void init(TaskAttemptContext job) throws IOException {
        // 获取driver中设置的结果输出目录
        String outDir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        baiduStream = fileSystem.create(new Path(outDir + "/baidu.log"));
        otherStream = fileSystem.create(new Path(outDir + "/other.log"));
    }

    @Override
    public void write(LongWritable longWritable, Text text) throws IOException, InterruptedException {
        String out = text.toString() + "\n";
        if (out.contains("baidu")) {
            baiduStream.write(out.getBytes());
        }else {
            otherStream.write(out.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(baiduStream);
        IOUtils.closeStream(otherStream);
    }
}