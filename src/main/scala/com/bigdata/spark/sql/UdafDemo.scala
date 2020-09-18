package com.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
  * 自定义聚合函数  求平均值
  */
object UdafDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("TransForm_2")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //自定义聚合函数
    //创建聚合对象
    val udaf = new MyAgeAvgFunction
    //注册聚合对象
    spark.udf.register("avgAge",udaf)

    //使用聚合函数
    val frame = spark.read.json("in/person.json")
    frame.createOrReplaceTempView("person")

    spark.sql("select avgAge(age) from person").show
    /* 结果
    +---------------------+
    |myageavgfunction(age)|
    +---------------------+
    |   23.333333333333332|
    +---------------------+
     */
    spark.close()
  }
}

/**
  * 声明 用户自定义函数
  * 1. 继承UserDefinedAggregateFunction
  * 2. 实现航发
  */
class MyAgeAvgFunction extends UserDefinedAggregateFunction {
  /**
    * 函数输入数据结构
    * @return
    */
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }

  /**
    * 计算时的数据结构
    * sum 总和
    * count 总数
    * @return
    */
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }

  /**
    * 函数返回的数据类型
    * @return
    */
  override def dataType: DataType = DoubleType

  /**
    * 函数是否稳定 相同的数据调用2次，返回值是否相等
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 计算前缓冲区初始化
    * 对sum和count初始化
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
    buffer(1)=0L
  }


  /**
    * 更新数据
    * 根据查询结果，更新缓冲区的数据
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  /**
    * 将多个节点的缓冲区合并
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // sum
    buffer1(0) = buffer1.getLong(0)+ buffer2.getLong(0)
    //count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

  }

  /**
    * 计算 最终结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}