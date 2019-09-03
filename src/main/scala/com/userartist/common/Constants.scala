package com.userartist.common

object Constants {
  //是否运行在本地
  val SPARK_RUN_ENVIMENT_ISLOCAL = ConfigurationManager.getProperty("isLocal");
  val SPARK_CLUSTER_MASTER = ConfigurationManager.getProperty("spark.clusterMasterURL");
}
