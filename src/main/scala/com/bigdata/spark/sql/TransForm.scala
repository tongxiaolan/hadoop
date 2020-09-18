package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/** 转换
  * rdd->df->ds
  * ds->df->rdd
  */
object TransForm {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("SqlDemo")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //创建RDD
    val rdd = spark.sparkContext.makeRDD(List((1,"zhangsan",20),(2,"lisi",17),(3,"wangwu",50)))

    //转换为df
    val df = rdd.toDF("id","name","age")

    // 转换为DS
    val ds = df.as[User]

    // 转换为df
    val df2 = ds.toDF()
    //转换为rdd
    val rdd1:RDD[Row] = df2.rdd

    rdd1.foreach(row =>{
      println(row.getString(1))
    })

    spark.close()
  }
}

case class User(id:Int,name:String,age:Int)