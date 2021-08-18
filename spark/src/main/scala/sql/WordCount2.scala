package sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WordCount2 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    sc.setLogLevel("WARN")

    //2.读取文件
    val fileDF: DataFrame = spark.read.text("D:\\data\\words.txt")
    val fileDS: Dataset[String] = spark.read.textFile("D:\\data\\words.txt")

    //fileDF.show()
    //fileDS.show()

    //3.对每一行按照空格进行切分并压平
    //fileDF.flatMap(_.split(" ")) //注意:错误,因为DF没有泛型,不知道_是String
    import spark.implicits._
    val wordDS: Dataset[String] = fileDS.flatMap((_: String).split(" ")) //注意:正确,因为DS有泛型,知道_是String

    //wordDS.show()
    /*
    +-----+
    |value|
    +-----+
    |hello|
    |   me|
    |hello|
    |  you|
    ...
     */

    //4.对上面的数据进行WordCount
    wordDS.groupBy("value").count().orderBy($"count".desc).show()
    sc.stop()
    spark.stop()

  }

}
