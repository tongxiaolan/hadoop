package com.bigdata.mr.writablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 小懒
 * @create 2020/7/10
 * @since 1.0.0
 */
public class SortReducer extends Reducer<FlowBeanCom,Text,Text,FlowBeanCom>{
    @Override
    protected void reduce(FlowBeanCom key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}