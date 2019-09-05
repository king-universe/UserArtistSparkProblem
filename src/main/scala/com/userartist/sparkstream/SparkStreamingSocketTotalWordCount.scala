package com.userartist.sparkstream

import com.userartist.appconf.AppConf
import com.userartist.sparkstream.SparkStreamingSocketWordCount.ssc
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * SparkStreaming接受socket数据，实现所有批次单词计数结果累加
  */
object SparkStreamingSocketTotalWordCount extends AppConf {

  def updateFunc(currentValues: Seq[Int], historyValues: Option[Int]): Option[Int] = {
    val newValue: Int = currentValues.sum + historyValues.getOrElse(0)
    Some(newValue)
  }

  def main(args: Array[String]): Unit = {
    ssc.checkpoint("./socket");
    //获取端口的数据
    val socketText: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //将数据进行拆分
    val wordsDstream: DStream[String] = socketText.flatMap(_.split(","))
    //对数据进行加工cheng(String,1)
    val wordAndOneDstream: DStream[(String, Int)] = wordsDstream.map((_, 1))

    val result: DStream[(String, Int)] = wordAndOneDstream.updateStateByKey(updateFunc);

    result.print();

    ssc.start()

    ssc.awaitTermination();

  }
}
