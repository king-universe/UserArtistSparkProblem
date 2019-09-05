package com.userartist.sparkstream

import com.userartist.appconf.AppConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * SparkStreaming开窗函数reduceByKeyAndWindow，实现单词计数
  */
object SparkStreamingSocketWordCountWindows extends AppConf{
  def main(args: Array[String]): Unit = {
    ssc.checkpoint("./socket");
    //获取端口的数据
    val socketText: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //将数据进行拆分
    val wordsDstream: DStream[String] = socketText.flatMap(_.split(","))
    //对数据进行加工cheng(String,1)
    val wordAndOneDstream: DStream[(String, Int)] = wordsDstream.map((_, 1))

    //使用开窗函数   第一个参数是进行的函数，第二个参数是多少时间的数据窗口大小，第三个是表示滑动窗口的时间间隔，也就意味着每隔多久计算一次
    val windowsWordCountDstream:DStream[(String,Int)] = wordAndOneDstream.reduceByKeyAndWindow((x: Int, y: Int) => (x + y), Seconds(10), Seconds(5))

    windowsWordCountDstream.print();

    ssc.start()

    ssc.awaitTermination();

  }
}
