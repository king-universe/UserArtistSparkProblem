package com.userartist.sparkstream

import com.userartist.appconf.AppConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

import scala.collection.immutable

/**
  * 这种receiver模式，刚开始的时候系统正常运行，没有发现问题，但是如果系统异常重新启动sparkstreaming程序后，
  * 发现程序会重复处理已经处理过的数据，这种基于receiver的方式，是使用Kafka的高级API，topic的offset偏移量在ZooKeeper中。
  * 这是消费Kafka数据的传统方式。
  * 这种方式配合着WAL机制可以保证数据零丢失的高可靠性，但是却无法保证数据只被处理一次，可能会处理两次。
  * 因为Spark和ZooKeeper之间可能是不同步的。
  * 官方现在也已经不推荐这种整合方式，我们使用官网推荐的第二种方式kafkaUtils的createDirectStream()方式。
  * 现在的pom文件不支持这种方式
  */
object SparkStreamingKafkaReceiver extends AppConf {
  def main(args: Array[String]): Unit = {
  val zkQuorum = "47.100.236.207:2181"
    //消费者组id
    val groupId = "sparkStreaming_group"
    //topic信息     //map中的key表示topic名称，map中的value表示当前针对于每一个receiver接受器采用多少个线程去消费数据
    val topics = Map("kafkaSpark" -> 1)
    val kafkaStreams: immutable.IndexedSeq[ReceiverInputDStream[(String, String)]] = (1 to 3).map(x => {
      val kafkaStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics);
      kafkaStream
    })

    val kafkaStream: DStream[(String, String)] = ssc.union(kafkaStreams);

    val kafkaData: DStream[String] = kafkaStream.map(_._2);

    val words = kafkaData.flatMap(_.split(","))

    val wordAndOne: DStream[(String, Int)] = words.map((_, 1));

    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _);

    result.print()

    ssc.start();

    ssc.awaitTermination()
  }
}
