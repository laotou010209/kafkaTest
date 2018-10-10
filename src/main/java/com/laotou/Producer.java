package com.laotou;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 生产者
 */
public class Producer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 该地址是集群的子集，用来探测集群。
        props.put("bootstrap.servers","gz237-111:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("yangfengbing", Integer.toString(i), Integer.toString(i)+"水电费等方式捣yang"));
            try {
                Thread.sleep(1000);
                System.out.println("************");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //producer.close();
    }
}
