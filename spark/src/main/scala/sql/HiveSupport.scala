package sql

import org.apache.spark.sql.SparkSession

object HiveSupport {
  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("HiveSupport")
      .master("local[*]")
      //.config("spark.sql.warehouse.dir", "hdfs://node01:8020/user/hive/warehouse")
      //.config("hive.metastore.uris", "thrift://node01:9083")
      .enableHiveSupport() //开启hive语法的支持
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    //查看有哪些表
    spark.sql("show tables").show()

    //创建表
    spark.sql("CREATE TABLE person (id int, name string, age int) row format delimited fields terminated by ' '")

    //加载数据,数据为当前SparkDemo项目目录下的person.txt(和src平级)
    spark.sql("LOAD DATA LOCAL INPATH 'SparkDemo/person.txt' INTO TABLE person")

    //查询数据
    spark.sql("select * from person ").show()

    spark.stop()
  }
}
