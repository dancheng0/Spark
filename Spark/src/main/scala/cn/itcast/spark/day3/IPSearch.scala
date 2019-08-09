package cn.itcast.spark.day3

import org.apache.spark.{SparkConf, SparkContext}

object IPSearch {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("search").setMaster("local")
    val sc = new SparkContext(conf)

  }

  def readData(path:String): Unit ={

  }

}
