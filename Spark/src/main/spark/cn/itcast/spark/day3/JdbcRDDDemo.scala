package cn.itcast.spark.day3

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ZX on 2016/4/12.
  *
  * 使用JDBC操作mysql数据库
  */
object JdbcRDDDemo {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/hello", "root", "123456")
    }
    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "SELECT * FROM a where id >= ? AND id <= ?",
      1, 4, 2,
      r => {
        val id = r.getInt(1)
        val name = r.getString(2)
        (id, name)
      }
    )
    val jrdd = jdbcRDD.collect()
    println(jdbcRDD.collect().toBuffer)
    sc.stop()
  }
}
