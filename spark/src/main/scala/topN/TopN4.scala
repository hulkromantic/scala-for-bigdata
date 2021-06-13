package topN

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TopN4 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.加载数据
    val logfile: RDD[String] = sc.textFile("D:\\teache.log")

    //3.处理数据
    //RDD[(学科, 老师)]
    val subjectAndTeacher: RDD[(String, String)] = logfile.map(url => {
      val strs: Array[String] = url.split("[/]")
      val subject:String =strs(2).split("[.]")(0)
      val teacher:String =strs(3)
      (subject,teacher)
    })

    import spark.implicits._

    //4.RDD转DF
    val subjectAndTeacherDF: DataFrame = subjectAndTeacher.toDF("subject","teacher")
    subjectAndTeacherDF.createOrReplaceTempView("t_teacher")

    val sql =
      """
        |select subject,teacher,count(*) counts
        |from t_teacher
        |group by subject,teacher
        |order by counts desc
      """.stripMargin

    //5.聚合查询
    val temp: DataFrame = spark.sql(sql)
    temp.show()
    temp.createOrReplaceTempView("temp")
/*

+-------+-------+------+
|subject|teacher|counts|
+-------+-------+------+
|bigdata|    tom|    15|
| javaee|  jerry|     9|
|bigdata|   jack|     6|
| javaee|   tony|     6|
|    php|   lucy|     4|
|bigdata|   andy|     2|
+-------+-------+------+
     */

    //6.对上面的临时表,按学科组内排序,可以使用我们之前学习的row_number() over()开窗函数
    val sql2 =
      """
        |select subject,teacher,counts, row_number() over(partition by subject order by counts desc) rank
        |from temp
      """.stripMargin
    spark.sql(sql2).show()

    /*
    +-------+-------+------+----+
    |subject|teacher|counts|rank|
    +-------+-------+------+----+
    | javaee|  jerry|     9|   1|
    | javaee|   tony|     6|   2|
    |bigdata|    tom|    15|   1|
    |bigdata|   jack|     6|   2|
    |bigdata|   andy|     2|   3|
    |    php|   lucy|     4|   1|
    +-------+-------+------+----+
     */
    sc.stop()
  }
}
