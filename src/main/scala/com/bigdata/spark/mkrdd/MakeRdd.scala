package com.bigdata.spark.mkrdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 生成rdd的方式有3种，
  * 1 从集合中创建  -->parallelize和makeRdd
  * 2 从外部文件系统中创建
  * 3 其他rdd创建
  */
object MakeRdd {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("MakeRdd")
    val sc = new SparkContext(conf)

    // 创建rdd 底层实现就是parallelize
    // 手动传入分区数，如果没有传，默认取'2'和可用核数（此处设置为3 在setMaster中）的最大值
    val listRdd = sc.makeRDD(List(1, 2, 3, 4, 5),3)
    listRdd.collect().foreach(println)

    // parallelize实现
    val arrayRdd = sc.parallelize(Array(1,2,3,4,5))
    arrayRdd.collect.foreach(println)

    // 从外部存储中创建  本地文件系统：file://    hdfs系统： hdfs://
    // 默认从文件中读取的数据都是字符串类型
    // 读取文件时，传递的分区参数为最小分区数，但是不一定就是这个数，取决于hadoop读取文件时的分片规则
    val fileRdd = sc.textFile("file:///data/input",3)


  }
}
