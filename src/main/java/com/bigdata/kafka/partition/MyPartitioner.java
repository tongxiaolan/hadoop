package com.bigdata.kafka.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区  如果需要自定义分区的话，可以参照
 *  org.apache.kafka.clients.producer.internals.DefaultPartitioner 的写法 获取活的分区
 * @author 小懒
 * @create 2020/8/24
 * @since 1.0.0
 */
public class MyPartitioner implements Partitioner{

    @Override
    public int partition(String s, Object key, byte[] bytes, Object value, byte[] bytes1, Cluster cluster) {
        return key.toString().hashCode() % 3;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}