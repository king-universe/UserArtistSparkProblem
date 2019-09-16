package com.userartist.spark

import com.userartist.appconf.AppConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object HmygAnalyzeSpark extends AppConf {
  def main(args: Array[String]): Unit = {
    val goodsRdd: RDD[String] = sc.textFile("file:///opt/data/hmyg_goods.txt")
    val gradeRdd: RDD[String] = sc.textFile("file:///opt/data/hmyg_grade_one.txt")

    val goodsOrginInfoRdd = goodsRdd.filter(row => {
      if (row.split(";").length > 11)
        true
      else
        false
    })
    //0007fc1daf034e7b8063434c2f00e45d;维达 Vinda 湿巾 冰爽醒肤 10片独立装X6包  量贩装;ca9191abc9d944d0b08cbe262bc00b10;17.90;16.00;10.00;湿巾 冰爽醒肤;0;45;4f22c9cd2e364965be079b3925d691ad;074d81c2c5244998b9b9687dca943215;d6588cd2808d4fb88ba0a7a70881e6b3
    val goodsInfoRdd: RDD[(String, (String, String, String, String, String, String, String, String, String, String, String))] = goodsOrginInfoRdd.map(row => {
      val colums: Array[String] = row.split(";");
      //(id,(name,grade_id,price,shopprice,maketPrice,remark,sales,stock,shopId,gradeTwoId,chiefId))
      (colums.apply(0), (colums(1), colums(2), colums(3), colums(4), colums(5), colums(6), colums(7), colums(8), colums(9), colums(10), colums(11)))
    })

    val gardeOrginRdd = gradeRdd.filter(row => {
      var a = false;
      if (row.split(";").length > 4)
        a = true
      else
        a = false
      a
    })

    val gradeInfoRdd: RDD[(String, (String, String, String, String))] = gardeOrginRdd.map(row => {
      val colums = row.split(";")
      //      (id,(name,parentId,level,chiefId))
      (colums.apply(0), (colums(1), colums(2), colums(3), colums(4)))
    })


    val gradeThreeAndGoodsIdRdd: RDD[(String, String)] = goodsInfoRdd.map(row => ((row._2._10, row._1)))

    val goodsIdAndGradeRdd: RDD[(String, String)] = gradeThreeAndGoodsIdRdd.join(gradeInfoRdd).map(rdd => {
      (rdd._1, rdd._2._1)
    })
    val gradeIdGroup = goodsIdAndGradeRdd.groupBy(_._1);
    val gradeAndCountRdd = gradeIdGroup.map(row => ((row._1, row._2.iterator.size)))
    gradeIdGroup.saveAsTextFile("hdfs://hadoop102:9000/gradeGroupData")
    gradeAndCountRdd.saveAsTextFile("hdfs://hadoop102:9000/gradeCountData")
  }
}
