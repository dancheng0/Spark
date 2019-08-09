package cn.itcast.spark.day4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by root on 2016/5/19.
  */
object SQLDemo {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SQLDemo")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","bigdata")

    val personRdd = sc.textFile("hdfs://node-1.itcast.cn:9000/person.txt").map(line =>{
      val fields = line.split(",")
      Person(fields(0).toLong, fields(1), fields(2).toInt)
    })

    //需要导入该隐式转换，不然无法使用
    import sqlContext.implicits._
    val personDf = personRdd.toDF

    personDf.registerTempTable("person")

    sqlContext.sql("select * from person where age >= 20 order by age desc limit 2").show()

    sc.stop()

  }
}

//这个一定要放在外面
case class Person(id: Long, name: String, age: Int)