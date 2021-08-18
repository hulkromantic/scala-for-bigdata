package topN

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object TopN3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkContext
    val conf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //2.加载数据
    val logfile: RDD[String] = sc.textFile("D:\\teache.log")

    //3.处理数据
    //RDD[((学科, 老师), 1)]
    val subjectAndTeacherAndOne: RDD[((String, String), Int)] = logfile.map((url: String) => {
      val strs: Array[String] = url.split("[/]")
      val subject: String = strs(2).split("[.]")(0)
      val teacher: String = strs(3)
      ((subject, teacher), 1)
    })

    //4.获取所有的学科
    val subjects: Array[String] = subjectAndTeacherAndOne.map((_: ((String, String), Int))._1._1).distinct().collect()

    //5.创建自定义分区器
    val partitioner = new SubjectPartitioner(subjects)

    //6.使用自定义的分区器进行聚合,传入key(学科, 老师),实际上是根据学科进行的分区
    // RDD[((学科, 老师), 次数)]
    //[((bigdata, tom), 2),((bigdata, andy), 3)...]
    //那么reduceByKey(partitioner,_+_)返回的RDD各个分区内的数据都是同一学科
    val subjectAndTeacherAndCount: RDD[((String, String), Int)] = subjectAndTeacherAndOne.reduceByKey(partitioner, (_: Int) + (_: Int))

    //7.那么既然上面的返回的RDD各个分区内都是同一学科的数据,所以接下来直接对各个分区进行排序即可
    val result: RDD[((String, String), Int)] =
      subjectAndTeacherAndCount.mapPartitions((iter: Iterator[((String, String), Int)]) => {
        //进来的iter就是各个分区的数据,即各个学科的数据
        iter.toList.sortBy((_: ((String, String), Int))._2).reverse.take(1).toIterator
      })

    result.collect().foreach(println)
    /*
     ((javaee,jerry),9)
        ((php,lucy),4)
        ((bigdata,tom),15)
         */
    sc.stop()

  }

  //借鉴源码,实现自定义分区器,传入所有学科,根据学科进行分区
  class SubjectPartitioner(subjects: Array[String]) extends Partitioner {
    //自定义一个分区规则(学科,分区编号)
    var partitionRule: mutable.Map[String, Int] = mutable.Map[String, Int]()
    var num = 0;
    for (s <- subjects) {
      partitionRule.put(s, num)
      num += 1
    }

    //重写方法,返回分区数
    def numPartitions: Int = subjects.length

    //重写方法,传入key,返回分区编号
    def getPartition(key: Any): Int = {
      val subject: String = key.asInstanceOf[(String, String)]._1
      val partitionNum: Int = partitionRule(subject)
      partitionNum
    }
  }

}
