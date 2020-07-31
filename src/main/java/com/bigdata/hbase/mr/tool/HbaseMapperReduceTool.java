package com.bigdata.hbase.mr.tool;

import com.bigdata.hbase.mr.mapper.ScanDataMapper;
import com.bigdata.hbase.mr.reducer.InsertDataReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * 把一个表（my_np:student）的数据导入到另外一张表（my_np:user）中
 * 要求：2个表结构必须一致
 * @author 小懒
 * @create 2020/7/31
 * @since 1.0.0
 */
public class HbaseMapperReduceTool implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(HbaseMapperReduceTool.class);

        // mapper
        TableMapReduceUtil.initTableMapperJob(
                "my_np:student",
                new Scan(),
                ScanDataMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job

        );
        //reducer
        TableMapReduceUtil.initTableReducerJob(
                "my_np:user",
                InsertDataReduce.class,
                job
        );


        boolean b = job.waitForCompletion(true);
        return b? JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}