package com.bigdata.spark.rdds.trans

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object PartitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("Map")
    val sc = new SparkContext(conf)

    val ListRdd = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))

    val partitionerRdd = ListRdd.partitionBy(new MyPartitioner(3))
    partitionerRdd.saveAsTextFile("output")


  }
}

/**
  * 自定义分区
  * @param partitions
  */
class MyPartitioner(partitions: Int) extends Partitioner {
  // 分区数
  override def numPartitions: Int = {
    partitions
  }

  // 获取分区
  override def getPartition(key: Any): Int = {
    1
  }
}
