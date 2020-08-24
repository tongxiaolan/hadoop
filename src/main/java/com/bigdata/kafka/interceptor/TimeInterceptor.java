package com.bigdata.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 拦截器：给值前边加时间戳
 * @author 小懒
 * @create 2020/8/24
 * @since 1.0.0
 */
public class TimeInterceptor implements ProducerInterceptor<String,String>{

    @Override
    public void configure(Map<String, ?> configs) {

    }

    /**
     * 拦截器执行的具体方法
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 创建新的record对象，并返回   修改value的值
        return new ProducerRecord<>(record.topic(), record.partition(), record.key(), System.currentTimeMillis() + "," + record.value());
    }

    /**
     * 接受ack方法
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }


}