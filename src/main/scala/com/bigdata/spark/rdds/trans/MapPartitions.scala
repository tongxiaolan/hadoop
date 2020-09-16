package com.bigdata.spark.rdds.trans

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Map")
    val sc = new SparkContext(conf)
    // 准备数据
    val listRdd = sc.makeRDD(1 to 10)

    // 先遍历分区，获取分区的数据，再遍历其中的数据*2
    // 内部的map是scala的map
    val mapPartitionsRdd = listRdd.mapPartitions(datas => {
      datas.map(data => data * 2)
    })
    mapPartitionsRdd.collect().foreach(println)
  }
}
