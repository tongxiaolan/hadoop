package com.bigdata.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * 最普通的consumer
 * @author 小懒
 * @create 2020/8/24
 * @since 1.0.0
 */
public class Myconsumer {
    public static void main(String[] args) {
        // 创建配置信息
        Properties properties = new Properties();
        // 连接的集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");

        //自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        //自动提交offset间隔  自动提交offset是true才生效
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // k,v反序列化类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdataGroup");

        // 自动重置offset 默认不读之前的 在2个条件下才会触发  1消费者组第一次去消费  2消费者组的offset不存在了(时间超过7天，offset所在数据被删掉了)
        // 如果想要从新消费数据，第一，组名需要换，第二，开启这个参数
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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
        }
    }

}