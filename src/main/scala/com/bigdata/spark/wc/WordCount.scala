package com.bigdata.spark.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * word count 实例
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 配置信息
    val conf = new SparkConf().setMaster("local[3]").setAppName("WordCount")
    // 获取sc
    val sc = new SparkContext(conf)
    // 读取文件
    val lines = sc.textFile("G:\\bootFiles\\words.txt")
    // 分割为一个一个的单词
    val words = lines.flatMap(_.split(" "))
    // 转换结构 hello -> (hello,1)
    val wordToOne = words.map((_,1))
    // 分组聚合
    val wordSum = wordToOne.reduceByKey(_+_)
    //计算结果
    val result = wordSum.collect()
    // 打印
    result.foreach(println)

  }
}
