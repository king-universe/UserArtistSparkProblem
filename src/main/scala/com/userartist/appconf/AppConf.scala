package com.userartist.appconf

import com.userartist.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait AppConf {
  val localClusterURL = "local[2]"
  val clusterMasterURL = Constants.SPARK_CLUSTER_MASTER;
  val conf = new SparkConf().setMaster(clusterMasterURL);
  val sparkSession = new SparkSession(conf);
  val sc = sparkSession.sparkContext;
  val sqlContext = sparkSession.sqlContext;
}
