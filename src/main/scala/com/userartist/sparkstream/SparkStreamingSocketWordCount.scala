package com.userartist.sparkstream

import com.userartist.appconf.AppConf
import org.apache.spark.sql.Dataset
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object SparkStreamingSocketWordCount extends AppConf {
  def main(args: Array[String]): Unit = {
    //获取端口的数据
    val socketText: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //将数据进行拆分
    val wordsDstream: DStream[String] = socketText.flatMap(_.split(","))
    //对数据进行加工cheng(String,1)
    val wordAndOneDstream:DStream[(String,Int)]=wordsDstream.map((_,1))

    //进行reduce计算
    val result:DStream[(String,Int)]=wordAndOneDstream.reduceByKey(_+_);

    //打印
    result.print();

    //开始  一定不能忘
    ssc.start();

    //等待，不写就直接结束了
    ssc.awaitTermination();
  }
}
