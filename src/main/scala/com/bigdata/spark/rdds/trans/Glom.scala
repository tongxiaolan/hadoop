package com.bigdata.spark.rdds.trans

import org.apache.spark.{SparkConf, SparkContext}

object Glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Map")
    val sc = new SparkContext(conf)
    // 准备数据
    val ListRdd = sc.makeRDD(List(1,2,3,4,5,6,7),2)
    val glomRdd = ListRdd.glom()
    glomRdd.collect().foreach(array=> {
      println(array.mkString(","))
    })
    /*
    结果为：
      1,2,3
      4,5,6,7
     */
  }
}
