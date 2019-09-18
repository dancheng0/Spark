package cn.itcast.spark.BlackNameSystem


import org.apache.spark.{SparkConf, SparkContext}

object BlackNameSystem {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("Local[2]").setAppName("BlackNameSystem")
    val sc = new SparkContext(conf)
    val blackNameList = sc.parallelize(Array("阿鱼","诺言"))

  }

}
