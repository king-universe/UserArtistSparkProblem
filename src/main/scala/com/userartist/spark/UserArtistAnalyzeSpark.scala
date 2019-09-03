package com.userartist.spark

import com.userartist.appconf.AppConf
import com.userartist.common.ConfigurationManager
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.sql.{Row, SparkSession}
import com.userartist.common.Constants

object UserArtistAnalyzeSpark extends AppConf {
  def main(args: Array[String]): Unit = {
    val sql = "select * from user_artist_data"
    val userArtistRDD = sqlContext.sql("sql");
    userArtistRDD.rdd.mapPartitions(partition => {
      val site: List[String] = List();
      return Iterator(site)
       }
    )
  }


}
