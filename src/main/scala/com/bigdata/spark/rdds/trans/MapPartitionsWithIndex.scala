package com.bigdata.spark.rdds.trans

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Map")
    val sc = new SparkContext(conf)
    // 准备数据
    val listRdd = sc.makeRDD(1 to 10)

    // 返回 （数据，分区号）的数据
    val indexRdd = listRdd.mapPartitionsWithIndex{
      case (num,datas) => {
        datas.map((_,num))
      }
    }

    indexRdd.collect().foreach(println)

  }
}
