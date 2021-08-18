package sql

import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object ReadDataSourceDemo {
  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.读取文件
    spark.read.json("D:\\data\\output\\json").show()
    spark.read.csv("D:\\data\\output\\csv").toDF("id", "name", "age").show()
    spark.read.parquet("D:\\data\\output\\parquet").show()
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    spark.read.jdbc(
      "jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "person", prop).show()
    sc.stop()
    spark.stop()

  }

}
