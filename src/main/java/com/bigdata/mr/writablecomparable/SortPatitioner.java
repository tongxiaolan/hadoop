package com.bigdata.mr.writablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author 小懒
 * @create 2020/7/10
 * @since 1.0.0
 */
public class SortPatitioner extends Partitioner<FlowBeanCom, Text> {


    @Override
    public int getPartition(FlowBeanCom flowBeanCom, Text text, int i) {
        String telPhone = text.toString();
        switch (telPhone.substring(0, 3)) {
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }

    }
}