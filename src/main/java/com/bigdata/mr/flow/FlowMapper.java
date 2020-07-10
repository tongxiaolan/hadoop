package com.bigdata.mr.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author 小懒
 * @create 2020/7/9
 * @since 1.0.0
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    private Text telphone = new Text();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String[] filds = line.toString().split("\t");
        telphone.set(filds[1]);
        long upFlow = Long.parseLong(filds[filds.length - 3]);
        long downFlow = Long.parseLong(filds[filds.length - 2]);
        flowBean.set(upFlow,downFlow);
        context.write(telphone,flowBean);
    }
}