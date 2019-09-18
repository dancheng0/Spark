package sparksql

import java.util.Properties

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hh")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val personRDD = sc.parallelize(Array("1 tom 5", "2 lilei 3", "3 kitty 7")).map(_.split(" "))
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    //将RDD映射到RowRDD
    val rowRDD = personRDD.map(p=>Row(p(0).toInt,p(1).trim(),p(2).toInt))
    //将schema信息映射到RowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD,schema)

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")

    //将数据追加的数据表
    personDataFrame.write.mode("append").jdbc("jdbc:mysql://192.168.72.239:3306/test","test.Students",prop)

    //停止spark
    sc.stop()
  }


}
