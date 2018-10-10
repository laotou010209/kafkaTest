package com.laotou

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 创建kafka生产者模拟随机生产数据
  */
object ScalaProducer {
  def main(args: Array[String]): Unit = {
    val topic="words"
    val brokers="192.168.200.10:9092"
    val prop = new Properties()
    prop.put("bootstrap.servers",brokers)
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("serializer.class","kafka.serializer.StringEncoder")
//    val kafkaConfig = new ProducerConfig(prop)
  val producer1: KafkaProducer[String, String] = new KafkaProducer(prop)

    //模拟数据
//    val array = Array("kafka kafka produce","kafka produce message","hello world hello","wordcount topK topK","hbase spark kafka")
    val array: Array[String] = new Array[String](5)
    array(0)="kafka kafka produce"
    array(1)="kafka produce message"
    array(2)="hello world hello"
    array(3)="wordcount topK topK"
    array(4)="hbase spark kafka"
    //生产数据,生产数据100条
    for(a <- 1 to 1000){
      //每次随机生产一条数据
      val i = (math.random*5).toInt
      producer1.send(new ProducerRecord[String, String](topic,array(i)))
      println(array(i))
      Thread.sleep(200)
    }
  }
}
