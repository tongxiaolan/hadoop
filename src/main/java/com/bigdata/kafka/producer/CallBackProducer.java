package com.bigdata.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 带回调的生产者demo
 * @author 小懒
 * @create 2020/8/24
 * @since 1.0.0
 */
public class CallBackProducer {

    public static void main(String[] args) {
        // 创建配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 创建producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 发送数据
        for (int i = 0; i < 10; i++) {
            // 成功返回recordMetadata 元数据信息  失败返回exception
            producer.send(new ProducerRecord<>("first", "bigdata---" + i)
                    , (recordMetadata, e) -> {
                        if (e == null) {
                            // 成功
                            System.out.println("发送的主题--" + recordMetadata.topic() + "分区--" + recordMetadata.partition() + "offset--" + recordMetadata.offset());
                        }
                    });
        }
        // 关闭资源
        producer.close();

    }
}