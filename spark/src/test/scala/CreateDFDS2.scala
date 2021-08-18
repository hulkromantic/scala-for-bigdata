import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CreateDFDS2 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[*]").appName("df_test").getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("WARN")

    val fileRDD: RDD[String] = sc.textFile("/Users/lh/wc.txt")
    val linesRDD: RDD[Array[String]] = fileRDD.map(_.split(" "))
    val rowRDD: RDD[Person] = linesRDD.map(line => Person(line(0).toInt,line(1),line(2).toInt))

    import session.implicits._

    val personRDD = rowRDD.toDF
    personRDD.show(20)
    personRDD.printSchema()
    sc.stop()
    session.stop()
  }

  case class Person(id: Int,name: String,age:Int)

}
