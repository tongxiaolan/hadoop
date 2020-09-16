package com.bigdata.spark.rdds.trans

import org.apache.spark.{SparkConf, SparkContext}

/**
  * map 算子遍历所有数据
  */
object Map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Map")
    val sc = new SparkContext(conf)
    val ListRdd = sc.makeRDD(1 to 10)
   // 遍历所有数据  并*2
    val nums = ListRdd.map(_*2)
    // 打印
    nums.collect().foreach(println)
  }
}
