package source

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DataSource {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setAppName("DataSourceTest").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")
    System.setProperty("HADOOP_USER_NAME", "root")

    //1.HadoopAPI
    println("HadoopAPI")
    val dataRDD: RDD[(Int, String)] = sc.parallelize(Array((1, "hadoop"), (2, "hive"), (3, "spark")))
    dataRDD.saveAsNewAPIHadoopFile("hdfs://node01:8020/spark_hadoop/",
      classOf[LongWritable],
      classOf[Text],
      classOf[TextOutputFormat[LongWritable, Text]])

    val inputRDD: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile(
      "hdfs://node01:8020/spark_hadoop/*",
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      conf = sc.hadoopConfiguration
    )
    inputRDD.map((_: (LongWritable, Text))._2.toString).foreach(println)

    //2.读取小文件
    println("读取小文件")
    val filesRDD: RDD[(String, String)] = sc.wholeTextFiles("D:\\data\\spark\\files", minPartitions = 3)
    val linesRDD: RDD[String] = filesRDD.flatMap((_: (String, String))._2.split("\\r\\n"))
    val wordsRDD: RDD[String] = linesRDD.flatMap((_: String).split(" "))
    wordsRDD.map(((_: String), 1)).reduceByKey((_: Int) + (_: Int)).collect().foreach(println)

    //3.操作SequenceFile
    println("SequenceFile")
    val dataRDD2: RDD[(Int, String)] = sc.parallelize(List((2, "aa"), (3, "bb"), (4, "cc"), (5, "dd"), (6, "ee")))
    dataRDD2.saveAsSequenceFile("D:\\data\\spark\\SequenceFile")
    val sdata: RDD[(Int, String)] = sc.sequenceFile[Int, String]("D:\\data\\spark\\SequenceFile\\*")
    sdata.collect().foreach(println)

    //4.操作ObjectFile
    println("ObjectFile")
    val dataRDD3: RDD[(Int, String)] = sc.parallelize(List((2, "aa"), (3, "bb"), (4, "cc"), (5, "dd"), (6, "ee")))
    dataRDD3.saveAsObjectFile("D:\\data\\spark\\ObjectFile")
    val objRDD: RDD[(Int, String)] = sc.objectFile[(Int, String)]("D:\\data\\spark\\ObjectFile\\*")
    objRDD.collect().foreach(println)

    sc.stop()
  }

}