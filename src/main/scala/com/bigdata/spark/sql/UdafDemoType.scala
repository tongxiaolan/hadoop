package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/**
  * 强类型的自定义聚合函数
  */
object UdafDemoType {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("TransForm_2")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //准备数据
    val frame = spark.read.json("in/person.json")
    val userDs = frame.as[UserBean]

    // 创建聚合函数对象
    val udaf = new MyAgeAvgFunctionByType

    //将聚合函数转换为查询的列
    val avgCol = udaf.toColumn.name("avgAge")
    userDs.select(avgCol).show
    /*
    结果
    +------------------+
    |            avgAge|
    +------------------+
    |23.333333333333332|
    +------------------+
     */

    spark.close()
  }
}

/**
  * 样例类  从文件中获取到数字时，无法确定数字大小，所以会给大的数字类型
  * @param name
  * @param age
  */
case class UserBean(name:String,age:BigInt)
case class AvgBuffer(var sum:BigInt,var count:Int)

/**
  * 声明用户自定义聚合函数（强类型）
  * 1. 继承Aggregator 设定泛型
  * 2. 实现方法
  */
class MyAgeAvgFunctionByType extends Aggregator [UserBean,AvgBuffer,Double]{
  /**
    * 初始化
    * @return
    */
  override def zero: AvgBuffer = {
    AvgBuffer(0,0)
  }

  /**
    * 聚合数据
    * 处理接受的数据
    * @param b
    * @param a
    * @return
    */
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  /**
    * 缓冲区的合并操作
    * @param b1
    * @param b2
    * @return
    */
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  /**
    * 最终计算
    * @param reduction
    * @return
    */
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  /**
    * 自定义样例类选 product
    * @return
    */
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  /**
    * 常规数据结构选择对应的
    * @return
    */
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

