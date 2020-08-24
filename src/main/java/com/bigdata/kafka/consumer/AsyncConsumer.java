package com.bigdata.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * @author 小懒
 * @create 2020/8/25
 * @since 1.0.0
 */
public class AsyncConsumer {
    public static void main(String[] args) {
        // 创建配置信息
        Properties properties = new Properties();
        // 连接的集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");

        // k,v反序列化类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //关闭自动提交 offset
        properties.put("enable.auto.commit", "false");
        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdataGroup");

        //创建consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 订阅主题  参数为集合，可以订阅多个  可以订阅不存在的topic,日志会有一个警告
        consumer.subscribe(Arrays.asList("first","second"));

        // 阻塞，循环拉取数据
        while (true) {
            //拉取数据   如果没有数据  等待100ms
            ConsumerRecords<String, String> records = consumer.poll(100);

            //处理数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("key--"+record.key()+" --value--"+record.value()+" --partiton--"+record.partition() +" --offset--"+record.offset());
            }

            //异步提交
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public  void  onComplete(Map<TopicPartition,
                                        OffsetAndMetadata> offsets, Exception exception) {
                    // 提交失败处理逻辑
                    if (exception != null) {
                        System.err.println("Commit  failed  for"  +
                                offsets);
                    }
                }
            });
        }
    }
}