package imooc.com

import org.apache.spark.sql.SparkSession

/**
 * SparkSession的使用
 */
object SparkSessionApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkSessionApp")
      .master("local[2]").getOrCreate()

    val people = spark.read.json("H:\\TestSpace\\Project\\B_HUA\\FileUnzip\\dataFlow\\DataFlowExport_20180921175856\\DirMapping.json")
    people.show()

    spark.stop()
  }
}
