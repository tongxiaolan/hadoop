package com.bigdata.spark.rdds.trans

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 去重
  */
object Distanct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Map")
    val sc = new SparkContext(conf)

    // 准备数据
    val listRdd = sc.makeRDD(List(1,2,1,4,4,6,7,2,9,4,9,5))
    // 去重 会发送shuffle
//    val sampleRdd = listRdd.distinct()
    // 重新分区，保存到2个分区
    val disRdd = listRdd.distinct(2)
    disRdd.collect().foreach(println)

  }
}
