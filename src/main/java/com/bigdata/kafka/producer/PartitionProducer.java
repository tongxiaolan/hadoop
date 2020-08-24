package com.bigdata.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 自定义分区的producer
 * @author 小懒
 * @create 2020/8/24
 * @since 1.0.0
 */
public class PartitionProducer {
    public static void main(String[] args) {
        // 创建配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 配置分区信息
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.bigdata.kafka.partition.MyPartitioner");

        //创建producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", "key" + i, "bitdata--" + i));
        }

        // 关闭连接
        producer.close();


    }

}