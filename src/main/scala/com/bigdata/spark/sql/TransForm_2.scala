package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 转换
  * rdd->ds
  * ds->rdd
  */
object TransForm_2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("TransForm_2")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //准备数据
    val rdd = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",17),(3,"wangwu",50)))
    val userRdd = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    //rdd->ds
    val ds = userRdd.toDS()
    // ds->rdd
    val rdd1 = ds.rdd
    //打印
    rdd1.foreach(println)

    spark.close()

  }
}
