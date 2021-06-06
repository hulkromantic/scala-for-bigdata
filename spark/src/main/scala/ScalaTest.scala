import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hello").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //val rdd:RDD[Int]= sc.parallelize(List(5,6,4,7,3,8,2,9,1,10))
    val fileRDD: RDD[String] = sc.textFile("/Users/lh/Downloads/spark_test.csv")
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split("\\\\001"))
    wordRDD.foreach(x=>println(x))
    sc.stop()
  }
}
