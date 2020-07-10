package com.bigdata.mr.writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入是 FlowDriver 的输出  文件结构为 telphone upflow downflow sumflow
 * @author 小懒
 * @create 2020/7/10
 * @since 1.0.0
 */
public class SortMapper extends Mapper<LongWritable, Text, FlowBeanCom, Text> {

    private FlowBeanCom flow = new FlowBeanCom();
    private Text telphone = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] filds = value.toString().split("\t");
        telphone.set(filds[0]);
        flow.setUpFlow(Long.parseLong(filds[1]));
        flow.setDownFlow(Long.parseLong(filds[2]));
        flow.setSumFlow(Long.parseLong(filds[3]));
        context.write(flow,telphone);
    }
}