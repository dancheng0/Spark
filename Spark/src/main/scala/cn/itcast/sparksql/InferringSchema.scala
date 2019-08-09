package cn.itcast.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object InferringSchema {
  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置APP名称
    val conf = new SparkConf().setAppName("SQL-1")
    //SQLContext要依赖SparkContext
    val sc = new SparkContext(conf);
    //创建 SQLContext
    val sqlContext = new SQLContext(sc)
    //从指定的地址创建RDD
    val lineRDD = sc.textFile(args(0)).map(_.split(" "))
    //创建case class
    //将RDD 和 case class关联
    val personRDD = lineRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))
    //导入隐式转换，如果不导入无法将RDD转换成DataFrame
    //将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF = personRDD.toDF
    //注册表
    personDF.registerTempTable("t_person")
    //传入SQL
    val df = sqlContext.sql("select * from t_person order by age desc limit 2")
    //将结果以json的方式存储到指定位置
    df.write.json(args(1))
    //停止sparkContext
    sc.stop()
  }

  //case class一定要放到外面

  case class Person(id: Int, name: String, age: Int)

}
