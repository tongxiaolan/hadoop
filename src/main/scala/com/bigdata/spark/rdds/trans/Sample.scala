package com.bigdata.spark.rdds.trans

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 采样
  */
object Sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Map")
    val sc = new SparkContext(conf)
    // 准备数据

    val listRdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10,11,12))
    // 参数1 是否放回  参数2 抽取的比例， 1则留下，0则全不要，不一定准确  参数三：随机数种子
    val sampleRdd = listRdd.sample(false,0.4,System.currentTimeMillis())
    sampleRdd.collect().foreach(println)

  }
}
