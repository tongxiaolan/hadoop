package com.bigdata.mr.partition;

import com.bigdata.mr.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 将统计结果按照手机归属地不同省份输出到不同文件中（分区）
 * 期望输出数据
    手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中

 getPartition（）返回的分区数必须小于等于设置的分区数
 如果小于，会有空分区，会浪费资源

 * @author 小懒
 * @create 2020/7/10
 * @since 1.0.0
 */
public class MyPartitioner extends Partitioner<Text,FlowBean>{

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String telphone = text.toString();
        switch (telphone.substring(0, 3)) {
            case "136" :
                return 0;
            case "137" :
                return 1;
            case "138" :
                return 2;
            case "139" :
                return 3;
            default:
                return 4;
        }
    }
}