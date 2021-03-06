package com.bigdata.mr.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 小懒
 * @create 2020/7/13
 * @since 1.0.0
 */
public class EtlMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    private LogBean logBean = new LogBean();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        paseLong(fields);
        if (logBean.isValid()) {
            k.set(logBean.toString());
            context.write(k, NullWritable.get());
            //计数器 成功的+1
            context.getCounter("ETL", "True").increment(1);
        }else {
            //计数器，不符合的+1
            context.getCounter("ETL", "False").increment(1);
        }


    }

    private void paseLong(String[] fields) {
        if (fields.length > 11) {

            // 2封装数据
            logBean.setRemote_addr(fields[0]);
            logBean.setRemote_user(fields[1]);
            logBean.setTime_local(fields[3].substring(1));
            logBean.setRequest(fields[6]);
            logBean.setStatus(fields[8]);
            logBean.setBody_bytes_sent(fields[9]);
            logBean.setHttp_referer(fields[10]);

            if (fields.length > 12) {
                logBean.setHttp_user_agent(fields[11] + " "+ fields[12]);
            }else {
                logBean.setHttp_user_agent(fields[11]);
            }

            // 大于400，HTTP错误
            if (Integer.parseInt(logBean.getStatus()) >= 400) {
                logBean.setValid(false);
            }
        }else {
            logBean.setValid(false);
        }
    }


}