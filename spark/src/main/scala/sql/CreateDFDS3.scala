package sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateDFDS3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.读取文件
    val fileRDD: RDD[String] = sc.textFile("D:\\data\\person.txt")
    val linesRDD: RDD[Array[String]] = fileRDD.map((_: String).split(" "))
    val rowRDD: RDD[Person] = linesRDD.map((line: Array[String]) => Person(line(0).toInt, line(1), line(2).toInt))

    //3.将RDD转成DF
    //注意:RDD中原本没有toDF方法,新版本中要给它增加一个方法,可以使用隐式转换
    import spark.implicits._

    //注意:上面的rowRDD的泛型是Person,里面包含了Schema信息
    //所以SparkSQL可以通过反射自动获取到并添加给DF
    val personDF: DataFrame = rowRDD.toDF
    personDF.show(10)
    personDF.printSchema()
    sc.stop()
    spark.stop()
  }

  case class Person(id: Int, name: String, age: Int)

}
