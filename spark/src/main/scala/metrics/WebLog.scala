package metrics

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object WebLog {
  def main(args: Array[String]): Unit = {

    //1.创建SC
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //2.加载数据
    val fileRDD: RDD[String] = sc.textFile("D:\\data\\access.log")

    //3.处理数据每一行按空格切分
    val linesRDD: RDD[Array[String]] = fileRDD.map((_: String).split(" "))

    //频繁使用的RDD可以进行缓存或持久化
    linesRDD.persist(StorageLevel.MEMORY_AND_DISK)

    //设置CheckpointDir
    sc.setCheckpointDir("./ckp") //实际开发中写HDFS

    //将RDD进行Checkpoint
    linesRDD.checkpoint()

    //4.统计指标
    //4.1统计网站pv
    //page view 网站页面浏览量(访问一次算一次)
    val pv: Long = linesRDD.count()
    println("pv: " + pv)

    val pvAndCount: RDD[(String, Int)] = linesRDD.map((line: Array[String]) => ("pv", 1))
      .reduceByKey((_: Int) + (_: Int))
    pvAndCount.collect().foreach(println)

    //4.2统计网站uv
    //user view 独立用户访问量(一个用户算一次访问,可以使用ip/Sessionid)
    //取出ip
    val ipRDD: RDD[String] = linesRDD.map((line: Array[String]) => line(0))
    //对ip进行去重
    val uv: Long = ipRDD.distinct().count()
    println("uv: " + uv)

    //4.3统计网站用户来源topN
    //统计refurl,表示来自于哪里?
    //数据预处理
    val filteredLines: RDD[Array[String]] = linesRDD.filter((_: Array[String]).length > 10)

    //取出refUrl
    val refurlRDD: RDD[String] = filteredLines.map((_: Array[String]) (10))

    //每个refurl记为1
    val refurlAndOne: RDD[(String, Int)] = refurlRDD.map(((_: String), 1))

    //按照key聚合
    val refurlAndCount: RDD[(String, Int)] = refurlAndOne.reduceByKey((_: Int) + (_: Int))

    //val top5: Array[(String, Int)] = refurlAndCount.top(5)
    //注意：top默认按照key排序，我们需要按照value次数排序
    //并且top是把数据拉回Driver再排序
    val result: RDD[(String, Int)] = refurlAndCount.sortBy((_: (String, Int))._2, false) //按照次数降序/逆序
    val top5: Array[(String, Int)] = result.take(5)
    top5.foreach(println)
    //pv: 14619

    //(pv,14619)

    //uv: 1050

    //("-",5205) //-表示来源于自己的网站

    //("http://blog.fens.me/category/hadoop-action/",547)

    //("http://blog.fens.me/",377)

    //("http://blog.fens.me/wp-admin/post.php?post=2445&action=edit&message=10",360)

    //("http://blog.fens.me/r-json-rjson/",274)
    sc.stop()
  }
}
