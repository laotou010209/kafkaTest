package com.laotou


import java.sql.{Connection, DriverManager, PreparedStatement}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  *sparkstreaming实时拉取kafka生成的数据,将统计结果写入mysql
  */
object ScalaSparkstreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordTest")
    val sc = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(1))
    //如果使用updateStateByKey的话，就必须指定checkPoint,并且设置了保存位置
    ssc.checkpoint("D:/spark_checkpoint")
    //配置kafka,通过KafkaUtils.createDirectStream读取kafka传递过来的数据
    val  topic=Set("words")
    val brokers="192.168.200.10:9092"
    val keySerializer="org.apache.kafka.common.serialization.StringSerializer"
    val valueSerializer="org.apache.kafka.common.serialization.StringSerializer"
    val serializerClass="kafka.serializer.StringEncoder"
    val kafkaParams=Map[String,String](
            "bootstrap.servers"->brokers,
            "key.serializer"->keySerializer,
            "value.serializer"->valueSerializer,
            "serializer.class"->serializerClass)
    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topic)
    //以窗口长度为10，滑动长度为3对Dstream进行截取,返回一个基于源Dstream的窗口批次计算后得到新的Dstream。
    var updateFunc2=updateFunc _
    val line: DStream[(String, Int)] = kafkaStream.flatMap(x => {
      Some(x._2.toString)
    }).flatMap(_.split(" ")).map((_, 1)).updateStateByKey[Int](updateFunc2).reduceByKeyAndWindow((a:Int, b:Int)=>a+b,Seconds(5),Seconds(5))
    //将DStream转成RDD，使用sortBy排序
    val data=line.transform(rdd=>{
      //降序处理后，取前5位
      val dataRDD: RDD[(String, Int)] = rdd.sortBy(t=>t._2,false)
      val sortResult: Array[(String, Int)] = dataRDD.take(5)

      val unit: Unit = sortResult.foreach(t => {
        //从这里将数据写入mysql
        //注意，conn和stmt定义为var不能是val
        var conn: Connection = null
        var ps : PreparedStatement = null
        //连接数据库
        conn = DriverManager.getConnection("jdbc:mysql://192.168.200.10:3306/result","root","010209")
        //插入数据,通过占位符的方式
        val sql2 = "insert into  words(word,number) values(?,?)"
        ps=conn.prepareStatement(sql2)
        ps.setString(1,t._1)
        ps.setInt(2,t._2)
        ps.executeUpdate()
      })
      println("--------------print top 5 end--------------")
      dataRDD
    })
    data.print()
    ssc.start()
    ssc.awaitTermination() //等待处理停止,stop()手动停止
  }

  /**
    * 根据相同的key进入到该方法
    * @param seqs  之前已经进入到该序列的所有的值
    * @param options 表示当前进入的值
    */
  def updateFunc(seqs:Seq[Int],options:Option[Int])={
    //需要将原来所有的值进行增加
    val sum = seqs.sum
    //拿到options中的值,使用getOrElse的目的是万一传过来的值，不是一个int类型的，那么Option会返回Nono，但是Nono无法也int进行相加，就
    //会出现异常，采用getOrElse就是为了解决这个问题
    val i = options.getOrElse(0)
    Some(sum+i)
  }

}
