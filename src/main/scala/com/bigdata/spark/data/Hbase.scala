package com.bigdata.spark.data

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 操作hbase数据
  */
object Hbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[5]").setAppName("hbase")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE,"student")

    //---------读取----------------
    val hbaseRdd = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    hbaseRdd.foreach{
      case (rowKey,result) => {
        val cells = result.rawCells()
        for(cell <- cells) {
          println(Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }


    //----------写入--------------------
    val dataRdd = sc.makeRDD(List(("1002","zhangsan"),("1003","lisi"),("1005","wangwu")))
    val putRdd = dataRdd.map{
      case (rowKey,data)=> {
        val  put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(data))
        (new ImmutableBytesWritable(Bytes.toBytes(rowKey)),put)
      }
    }
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"student")
    putRdd.saveAsHadoopDataset(jobConf)


    sc.stop()
  }
}
