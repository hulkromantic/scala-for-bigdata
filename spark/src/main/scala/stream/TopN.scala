package stream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TopN {
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("topN").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val streamingContext = new StreamingContext(sparkContext, Seconds(5)) //5表示5秒中对数据进行切分形成一个RDD
    //2.监听Socket接收数据
    //ReceiverInputDStream就是接收到的所有的数据组成的RDD,封装成了DStream,接下来对DStream进行操作就是对RDD进行操作
    val dataStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 6666)
    //3.操作数据
    val wordDStream: DStream[String] = dataStream.flatMap(_.split(" "))
    val wordAndOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
    //4.使用窗口函数进行WordCount计数
    val wordAndCountDStream: DStream[(String, Int)] = wordAndOneDStream.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(10), Seconds(5))
    val sortDStream: DStream[(String, Int)] = wordAndCountDStream.transform(rdd => {
      val sortRDD: RDD[(String, Int)] = rdd.sortBy(_._2, false) //逆序/降序
      println("===============top5==============")
      sortRDD.take(5).foreach(println)
      sortRDD
    })

    sortDStream.print()//No output operations registered, so nothing to execute
    //开启
    streamingContext.start()
    //等待优雅停止
    streamingContext.awaitTermination()

  }

}
