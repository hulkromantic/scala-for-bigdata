package topN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopN2 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkContext
    val conf: SparkConf = new SparkConf().setAppName("TopN").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //2.加载数据
    val logfile: RDD[String] = sc.textFile("D:\\data\\teache.log")

    //3.处理数据
    //RDD[((学科, 老师), 1)]
    val subjectAndTeacherAndOne: RDD[((String, String), Int)] = logfile.map(url => {
      val strs: Array[String] = url.split("[/]")
      val subject: String = strs(2).split("[.]")(0)
      val teacher: String = strs(3)
      ((subject, teacher), 1)
    })

    //4.根据key(学科, 老师),进行聚合
    //RDD[((学科, 老师), 次数)]
    //[((bigdata, tom), 2),((bigdata, andy), 3)...]
    val subjectAndTeacherAndCount: RDD[((String, String), Int)] = subjectAndTeacherAndOne.reduceByKey(_ + _)

    //5.根据学科进行分组
    //RDD[(bigdata, [((bigdata, tom), 2),((bigdata, andy), 3)...]....)]
    val groupBySubjectRDD: RDD[(String, Iterable[((String, String), Int)])] = subjectAndTeacherAndCount.groupBy(_._1._1)

    //6.组内排序
    //RDD[(学科, List[(老师, 次数)])]
    //_.toList:_表示Iterable
    //sortBy(_._2):_表示List中的元素((String, String), Int)),_2表示Int即次数
    //map(t=>(t._1._2,t._2)):t表示((String, String), Int)),t._1._2表示String即老师,t._2表示Int即次数
    val groupBySubjectSortByCountRDD: RDD[(String, List[(String, Int)])] =
      groupBySubjectRDD.mapValues(
        _.toList.sortBy(_._2).reverse.map(t => (t._1._2, t._2)))

    //7.收集结果
    groupBySubjectSortByCountRDD.collect().foreach(println)
    /*
    (javaee,List((jerry,9), (tony,6)))
    (php,List((lucy,4)))
    (bigdata,List((tom,15), (jack,6), (andy,2)))
    }
    */
    sc.stop()
  }
}