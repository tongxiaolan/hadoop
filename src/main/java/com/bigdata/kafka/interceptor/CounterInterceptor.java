package com.bigdata.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 统计成功条数和失败条数的拦截器
 * @author 小懒
 * @create 2020/8/24
 * @since 1.0.0
 */
public class CounterInterceptor implements ProducerInterceptor<String,String>{

    int success;
    int error;

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    /**
     * 根据ack,对成功/失败的数据计数  metadata!=null  成功，  exception!=null  失败
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            success++;
        }else {
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("success message：" + success);
        System.out.println("error message：" + error);
    }


}