package com.bigdata.spark.rdds.trans

import org.apache.spark.{SparkConf, SparkContext}

object FlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Map")
    val sc = new SparkContext(conf)
    // 准备数据
    val listRdd = sc.makeRDD(Array(List(1,2),List(3,4),List(5,6,7,8)))

    val flatMapRdd = listRdd.flatMap(datas => datas)
    flatMapRdd.foreach(println)

  }
}
