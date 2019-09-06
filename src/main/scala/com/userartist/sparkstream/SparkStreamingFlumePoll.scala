package com.userartist.sparkstream

import java.net.InetSocketAddress

import com.userartist.appconf.AppConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}


//这是flume 的配置文件
/**
a1.sources = r1
a1.sinks = k1
a1.channels = c1

#source
a1.sources.r1.channels = c1
a1.sources.r1.type = spooldir
#监听该目录下的文件变化
a1.sources.r1.spoolDir = /opt/data
a1.sources.r1.fileHeader = true

#channel
a1.channels.c1.type =memory
a1.channels.c1.capacity = 20000
a1.channels.c1.transactionCapacity=5000

#sinks
a1.sinks.k1.channel = c1
a1.sinks.k1.type = org.apache.spark.streaming.flume.sink.SparkSink
a1.sinks.k1.hostname=0.0.0.0
#a1.sinks.k1.hostname=127.0.0.1
a1.sinks.k1.port = 9999
a1.sinks.k1.batchSize= 2000
  */


//启动命令   bin/flume-ng agent -n a1 -c conf -f conf/flume-poll-spark.conf -Dflume.root.logger=info,console




/**
  * 如果是远程主机，一定要配置flume的conf配置文件中的bind为0.0.0.0
  */
object SparkStreamingFlumePoll extends AppConf {
  def main(args: Array[String]): Unit = {
    //poll拉模式获取所有管道中的数据
    val pollingStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, "47.100.236.207", 9999);


    //如果是多flume模式则为
    /*val address=List(new InetSocketAddress("node1",8888),new InetSocketAddress("node2",8888),new InetSocketAddress("node3",8888))
    val pollingStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc,address,StorageLevel.MEMORY_AND_DISK_SER_2);*/



    //event是flume中传输数据的最小单元，event中数据结构：{"headers":"xxxxx","body":"xxxxxxx"}
    val flume_data: DStream[String] = pollingStream.map(flumeEvent => new String((flumeEvent.event.getBody.array())))


    val splitData: DStream[String] = flume_data.flatMap(_.split(","))


    val wordMapAndCount: DStream[(String, Int)] = splitData.map((_, 1));

    val result: DStream[(String, Int)] = wordMapAndCount.reduceByKey(_ + _);


    val sortedDstream: DStream[(String, Int)] = result.transform(rdd => {
      //按照单词次数降序
      val sortRdd: RDD[(String, Int)] = rdd.sortBy(_._2, false);
      val top3WordRdd:Array[(String,Int)]=sortRdd.take(30);

      //打印
      println("=================top3===============开始")
      top3WordRdd.foreach(println)
      println("=================top3===============结束")

      sortRdd
    })

    sortedDstream.foreachRDD(rdd=>());
    ssc.start();

    ssc.awaitTermination();

  }
}
