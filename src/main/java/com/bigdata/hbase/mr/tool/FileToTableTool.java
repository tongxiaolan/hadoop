package com.bigdata.hbase.mr.tool;

import com.bigdata.hbase.mr.mapper.FileToTableMapper;
import com.bigdata.hbase.mr.reducer.InsertDataReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

/**
 * 将指定路径文件输入到表中  my_np:student
 * @author 小懒
 * @create 2020/8/1
 * @since 1.0.0
 */
public class FileToTableTool implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(FileToTableTool.class);

        FileInputFormat.setInputPaths(job,new Path("/data/user.txt"));

        job.setMapperClass(FileToTableMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);


        //reducer
        TableMapReduceUtil.initTableReducerJob(
                "my_np:student",
                InsertDataReduce.class,
                job
        );

        boolean b = job.waitForCompletion(true);
        return b ? JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}