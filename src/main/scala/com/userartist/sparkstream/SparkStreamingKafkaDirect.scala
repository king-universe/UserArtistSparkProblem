package com.userartist.sparkstream

import com.userartist.appconf.AppConf
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}


/**
  * kafka如果需要外网访问需要在kafka的server.properties配置文件中配置
  *
    host.name=阿里云内网地址      #kafka绑定的interface
    advertised.listeners=PLAINTEXT://阿里云外网映射地址:9092    # 注册到zookeeper的地址和端口
  *
  */
object SparkStreamingKafkaDirect extends AppConf {
  def main(args: Array[String]): Unit = {
//    val zkQuorum = "47.100.236.207:2181"
    //消费者组id
    val groupId = "kafkaSparkGroup1"
    //topic信息     //map中的key表示topic名称，map中的value表示当前针对于每一个receiver接受器采用多少个线程去消费数据
    val topics = List("kafkaSpark")
    //4、接受topic的数据
    //配置kafka相关参数
    val kafkaParams : Map[String,Object]=Map[String,Object](
    "bootstrap.servers" -> "47.100.236.207:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kafkaSparkGroup1",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean));


  val kafkaStream :  InputDStream[ConsumerRecord[String,String]] =  KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String, String](topics,kafkaParams))

    //    val kafkaStream: DStream[(String, String)] = ssc.union(kafkaStreams);

    val kafkaData: DStream[String] = kafkaStream.map(_.value());

    val words = kafkaData.flatMap(_.split(","))

    val wordAndOne: DStream[(String, Int)] = words.map((_, 1));

    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _);

    result.print()

    ssc.start();

    ssc.awaitTermination()
  }
}
