package topN

import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkConf, SparkContext}

object TopN1 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //2.加载数据
    val logfile: RDD[String] = sc.textFile("D:\\data\\teache.log")
    //3.处理数据
    val teacherAndOne: RDD[(String, Int)] = logfile.map(url => {
      val index: Int = url.lastIndexOf("/")
      val teacher: String = url.substring(index + 1)
      (teacher, 1)
    })

    //4.聚合
    val teacherAndCount: RDD[(String, Int)] = teacherAndOne.reduceByKey(_ + _)
    //5.排序
    val result: RDD[(String, Int)] = teacherAndCount.sortBy(_._2, false)
    result.collect().foreach(println)

    /*
    (tom,15)
    (jerry,9)
    (tony,6)
    (jack,6)
    (lucy,4)
    (andy,2)
     */

    sc.stop()
  }
}
