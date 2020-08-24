package com.bigdata.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * 使用拦截器的生产者
 * @author 小懒
 * @create 2020/8/24
 * @since 1.0.0
 */
public class InterceptorProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 添加拦截器  按照添加的顺序执行
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.bigdata.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.bigdata.kafka.interceptor.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        // 创建生产者
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        // 发送数据
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("first", "bigdata--" + i));
        }

        // 关闭资源  TODO  注意，此处调用以后，拦截器中的close方法才会跟着调用
        producer.close();

    }
}