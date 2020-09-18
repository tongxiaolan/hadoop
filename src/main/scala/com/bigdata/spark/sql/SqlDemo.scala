package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * spark sql的样例
  * sparksql需要单独添加pom依赖
  */
object SqlDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("SqlDemo")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val frame = spark.read.json("in/person.json")
//    frame.show()
    frame.createOrReplaceTempView("person")
    //采用sql方式查询
    spark.sql("select * from person").show()
    spark.close()
  }
}
