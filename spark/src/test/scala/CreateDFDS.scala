import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

object CreateDFDS {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[*]").appName("test_df").getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("WARN")

    val fileRDD: RDD[String] = sc.textFile("/Users/lh/wc.txt")
    val lineRDD: RDD[Array[String]] = fileRDD.map(_.split(" "))
    val rawRDD: RDD[Row] = lineRDD.map((line: Array[String]) => Row(line(0).toInt, line(1), line(2).toInt))

    val schema: StructType = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))
    val dataFrame: DataFrame = session.createDataFrame(rawRDD, schema)
    dataFrame.show(20)
    dataFrame.printSchema()
    sc.stop()
    session.stop()
  }

}
