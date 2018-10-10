package com.laotou;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Arrays;
import java.util.Properties;

/**
 * 消费者
 */
public class Consumer {
//    public static void main(String[] args) {
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "gz237-111:9092");
//        props.put("group.id", "test");// cousumer的分组id
//        props.put("auto.offset.reset", "earliest");
//        props.put("enable.auto.commit", "true");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// 反序列化器
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
//        consumer.subscribe(Arrays.asList("yangfengbing"));// 订阅的topic,可以多个
//        while (true) {
//            System.out.println("$$$$$$$$$$$$$$$$$$");
//            ConsumerRecords<String, String> records = consumer.poll(100);
//            System.out.println("@@@@@@@@@@@@@@@@@@2");
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf(record.value());
//                System.out.println();
//            }
//        }
//    }
}
