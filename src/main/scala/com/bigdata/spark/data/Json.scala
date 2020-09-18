package com.bigdata.spark.data

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 解析json 需要符合每行为一个json对象  文件内容如下
   {"name":"zhangsan","age":10}
   {"name":"lisi","age":20}
   {"name":"wangwu","age":40}
  */
object Json {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Map")
    val sc = new SparkContext(conf)

    val json = sc.textFile("in/person.json")

    import scala.util.parsing.json.JSON
    val result = json.map(JSON.parseFull)
    result.foreach(println)
    sc.stop()
  }
}
