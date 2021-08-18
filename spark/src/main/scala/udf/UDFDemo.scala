package udf

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}

object UDFDemo {
  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.读取文件
    val fileDS: Dataset[String] = spark.read.textFile("D:\\data\\udf.txt")
    fileDS.show()

    /*
    +----------+
    |     value|
    +----------+
    |helloworld|
    |       abc|
    |     study|
    | smallWORD|
    +----------+
     */
    /*

     将每一行数据转换成大写
     select value,smallToBig(value) from t_word
     */

    //注册一个函数名称为smallToBig,功能是传入一个String,返回一个大写的String
    spark.udf.register("smallToBig", (str: String) => str.toUpperCase())

    fileDS.createOrReplaceTempView("t_word")

    //使用我们自己定义的函数
    spark.sql("select value,smallToBig(value) from t_word").show()

    /*
    +----------+---------------------+
    |     value|UDF:smallToBig(value)|
    +----------+---------------------+
    |helloworld|           HELLOWORLD|
    |       abc|                  ABC|
    |     study|                STUDY|
    | smallWORD|            SMALLWORD|
    +----------+---------------------+
     */

    sc.stop()
    spark.stop()

  }

}
