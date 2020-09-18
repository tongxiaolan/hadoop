package com.bigdata.spark.data

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 操作mysql数据库
  */
object Mysql {
  def main(args: Array[String]): Unit = {
    // sparkContext
    val conf = new SparkConf().setMaster("local[3]").setAppName("Mysql")
    val sc = new SparkContext(conf)

    //mysql链接信息
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop101:3306/test"
    val userName = "root"
    val passWd = "root"
    val sql = "select name,age from user where id >= ? and id <= ?"

    // 创建jdbcRdd
    // 参数：1 sc,2 connect链接，3:sql, 4:数据范围起始，5：数据范围结束，6：结果处理函数
    val jcrdd = new JdbcRDD(
      sc,
      ()=> {
        Class.forName(driver)
        DriverManager.getConnection(url,userName,passWd)
      },
      sql,
      1,
      3,
      2,
      (rs) => {
        println(rs.getString(1)+" , " + rs.getInt(2))
      }
    )
//    jcrdd.collect()


    // 写数据
    //无法保证顺序
    val dataRdd = sc.makeRDD(List(("zhaoliu",30,4),("wagnqi",19,5),("heshang",90,6)))
    /*
      注意 此处链接在循环内部开启，浪费性能，但是如果把链接提取出来的话，因为conn变量是在
      driver中创建，foreach的代码是在executor中执行，所以conn需要序列化，但是conn没有办法序列化
      此处给出解决办法：使用foreachPatitions，每个分区开启一个链接
      */
    // foreach解决
    /*dataRdd.foreach{
      case (name,age,id) => {
        Class.forName(driver)
        val connection = DriverManager.getConnection(url,userName,passWd)
        val insertSql = "insert into user (name,age,id) values (?,?,?)"
        val statement = connection.prepareStatement(insertSql)
        statement.setString(1,name)
        statement.setInt(2,age)
        statement.setInt(3,id)
        statement.executeUpdate()

        statement.close()
        connection.close()
      }
    }*/

    /*
      *循环分区
      */
    dataRdd.foreachPartition(datas=> {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url,userName,passWd)
      val insertSql = "insert into user (name,age,id) values (?,?,?)"
      datas.foreach {
        case (name,age,id)=>{
          val statement = connection.prepareStatement(insertSql)
          statement.setString(1,name)
          statement.setInt(2,age)
          statement.setInt(3,id)
          statement.executeUpdate()
          statement.close()
        }
      }
      connection.close()
    })

    sc.stop()
  }
}
