package com.userartist.appconf

import com.userartist.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait AppConf {

  val localClusterURL = "local[2]"
  val clusterMasterURL = Constants.SPARK_CLUSTER_MASTER;
  val conf = new SparkConf().setMaster(localClusterURL);
  val sparkSession : SparkSession=SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();
  val sc = sparkSession.sparkContext;
  sc.setLogLevel("WARN");
  val sqlContext = sparkSession.sqlContext;
  val ssc=new StreamingContext(sc,Seconds(5));

}
