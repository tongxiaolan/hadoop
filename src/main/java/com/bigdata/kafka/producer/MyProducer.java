package com.bigdata.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 *
 * 普通的生产者demo
 * @author 小懒
 * @create 2020/8/24
 * @since 1.0.0
 */
public class MyProducer {
    public static void main(String[] args) {
        //  创建kafka生产者配置信息
        Properties properties = new Properties();

        //  配置信息 可以也可以直接从kafka配置类中拿属性名称  BOOTSTRAP_SERVERS_CONFIG和k,v序列化需要配，其他都是默认值，可以不配
        //kafka集群，broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        // ack应答级别
        properties.put("acks", "all");
        //重试次数
        properties.put("retries", 3);
        // 批次大小和等待时间满足其一就发送
        //批次大小 16k
        properties.put("batch.size", 16384);
        //等待时间 1毫秒
        properties.put("linger.ms", 1);
        //RecordAccumulator缓冲区大小  32m
        properties.put("buffer.memory", 33554432);
        // k v 序列化类
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", "bigdata---"+i));
        }

        // 关闭资源
        producer.close();

    }

}