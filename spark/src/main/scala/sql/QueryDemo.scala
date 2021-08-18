package sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object QueryDemo {
  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //2.读取文件
    val fileRDD: RDD[String] = sc.textFile("D:\\data\\person.txt")
    val linesRDD: RDD[Array[String]] = fileRDD.map((_: String).split(" "))
    val rowRDD: RDD[Person] = linesRDD.map((line: Array[String]) => Person(line(0).toInt, line(1), line(2).toInt))

    //3.将RDD转成DF
    //注意:RDD中原本没有toDF方法,新版本中要给它增加一个方法,可以使用隐式转换
    import spark.implicits._

    //注意:上面的rowRDD的泛型是Person,里面包含了Schema信息
    //所以SparkSQL可以通过反射自动获取到并添加给DF
    val personDF: DataFrame = rowRDD.toDF
    personDF.show(10)
    personDF.printSchema()

    //=======================SQL方式查询=======================

    //0.注册表
    personDF.createOrReplaceTempView("t_person")

    //1.查询所有数据
    spark.sql("select * from t_person").show()

    //2.查询age+1
    spark.sql("select age,age+1 from t_person").show()

    //3.查询age最大的两人
    spark.sql("select name,age from t_person order by age desc limit 2").show()

    //4.查询各个年龄的人数
    spark.sql("select age,count(*) from t_person group by age").show()

    //5.查询年龄大于30的
    spark.sql("select * from t_person where age > 30").show()

    //=======================DSL方式查询=======================

    //1.查询所有数据
    personDF.select("name", "age")

    //2.查询age+1
    personDF.select($"name", $"age" + 1)

    //3.查询age最大的两人
    personDF.sort($"age".desc).show(2)

    //4.查询各个年龄的人数
    personDF.groupBy("age").count().show()

    //5.查询年龄大于30的
    personDF.filter($"age" > 30).show()

    sc.stop()
    spark.stop()

  }

  case class Person(id: Int, name: String, age: Int)

}
