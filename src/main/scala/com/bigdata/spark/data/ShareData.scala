package com.bigdata.spark.data

import java.util

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 共享变量
  * 累加器：共享只写变量
  */
object ShareData {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[5]").setAppName("ShareData")
    val sc = new SparkContext(sparkConf)

    val dataRdd = sc.makeRDD(List(1,2,3,4),2)

    /*该种写法结果为0，因为计算是在executor端，计算完数据以后无法把数据返回driver
    var sum = 0;
    dataRdd.foreach(i=>sum = sum + i)*/

    //累加器
    val accumulator = sc.longAccumulator
    dataRdd.foreach{
      case i => {
        accumulator.add(i)
      }
    }
    println(accumulator.value)


    //使用自定义累加器-----------------
    val wordRdd = sc.makeRDD(List("hello","hive","spark","bigdata"),2)
    val wordAccumulator = new WordAccumulator()
    // 注册累加器
    sc.register(wordAccumulator)
    //使用
    wordRdd.foreach{
      case(word) => {
        // 执行累加器
        wordAccumulator.add(word)
      }
    }
    println(wordAccumulator.value)



    //----------------广播变量---------------------
    val list = List((1,1),(2,2),(3,3))
    val broadcastList = sc.broadcast(list)
    wordRdd.foreach{
      case (word) => {
        for(t <- broadcastList.value) {
          println(word+":"+t._1+"-->"+t._2)
        }
      }
    }

    sc.stop()
  }
}

/**
  * 自定义累加器
  * 1. 继承AccumulatorV2  参数： 输入类型 ,输出类型
  * 2. 实现抽象方法
  */
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  val list = new util.ArrayList[String]()

  /**
    * 当前累加器是否为初始化状态
    * @return
    */
  override def isZero: Boolean = list.isEmpty

  /**
    * 复制累加器
    * @return
    */
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = ???

  /**
    * 重置累加器
    */
  override def reset(): Unit = list.clear()

  /**
    * 累加数据
    * @param v
    */
  override def add(v: String): Unit = {
    if(v.contains("h")) {
      list.add(v)
    }
  }

  /**
    * 合并累加器
    * @param other
    */
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  /**
    * 获取累加器结果
    * @return
    */
  override def value: util.ArrayList[String] = list
}