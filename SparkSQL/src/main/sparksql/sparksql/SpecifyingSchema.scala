package sparksql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.sql.{Row, SQLContext, types}
import org.apache.spark.{SparkConf, SparkContext}

object SpecifyingSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQL-2");
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val personRDD = sc.textFile(args(0)).map(_.split(" "))
    //通过StructType直接指定每个字段的schema
    val schema = types.StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))
    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt))
    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    //注册表
    personDataFrame.registerTempTable("t_person")
    //执行SQL
    val df = sqlContext.sql("select * from t_person  order by age desc limit 4")
    //将结果以json的方式存储到指定的位置
    df.write.json(args(1))
    //停止sparkContext
    sc.stop()

  }

}
