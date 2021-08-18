import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.util.Arrays
import org.apache.spark.{SparkConf, SparkContext}

import java.util

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val wordAndOne: RDD[(String, Int)] = sc.textFile("/Users/lh/wc.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).persist(StorageLevel.MEMORY_AND_DISK_SER)
    wordAndOne.foreach(println(_))
    val tuples: RDD[String] = sc.textFile("/Users/lh/wc.txt").flatMap(_.split(" "))
    val tuples2: RDD[Array[String]] = sc.textFile("/Users/lh/wc.txt").map(_.split(" "))
    //val count: RDD[Array[String]] = sc.textFile("/Users/lh/wc.txt").map(_.split(" "))
    //println(count)
    /*
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 2)

    val result1: Int = rdd1.aggregate(0)(_ + _, _ + _) //45
    val result2: Int = rdd1.aggregate(10)(_ + _, _ + _) //75
    println(result1)
    println(result2)

          wordAndOne saveAsNewAPIHadoopFile("hdfs://node01:8020/spark_hadoop/",
          classOf[LongWritable],
          classOf[Text],
          classOf[TextOutputFormat[LongWritable, Text]])
         */

  }
}
