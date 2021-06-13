package analytics

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object IPMapSQL {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val sparkSession: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()
    val sparkContext: SparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel("WARN")
    //2.加载ip规则文件
    val ipFile: RDD[String] = sparkContext.textFile("/Users/lh/ip.txt")
    //3.获取ip起始范围(2)、结束范围(3)、城市信息(4,5,6,7,8)、经度(13)、维度(14)
    val lineArr: RDD[Array[String]] = ipFile.map(_.split("\\|"))
    //RDD[(ip起始值, ip结束值, 城市信息, 经度, 维度)]
    val ipRuleRDD: RDD[(String, String, String, String, String)] = lineArr.map(x => (x(2), x(3), x(4) + x(5) + x(6) + x(7) + x(8), x(13), x(14)))
    import sparkSession.implicits._
    //4.将RDD转换成DF
    val ipRulesDF: DataFrame = ipRuleRDD.toDF("startNum", "endNum", "city", "longitude", "latitude")
    //5.加载日志文件
    val logFile: RDD[String] = sparkContext.textFile("/Users/lh/20190121000132.394251.http.format")
    //6.获取日志中的ip并转为数字最后再将RDD转换成DF
    val ipDF: DataFrame = logFile.map(_.split("\\|")).map(arr => IPUtils.ipToLong(arr(1))).toDF("ipNum")
    //7.注册表t_rules、t_ips
    ipRulesDF.createOrReplaceTempView("t_ipRules")
    ipDF.createOrReplaceTempView("t_ips")
    //8.关联查询
    val sql: String =
      """
        |select city,longitude,latitude,count(*) counts
        |from t_ipRules
        |join t_ips
        |on ipNum>= startNum and ipNum <= endNum
        |group by city,longitude,latitude
        |order by counts desc
    """.stripMargin
    //sparkSession.sql(sql).show()
  }

  object IPUtils {
    //方法接收一个String类型的IP如:192.168.100.100,返回一个数字如:3232261220
    def ipToLong(ip: String): Long = {
      //注意:IP个原始面貌:
      //10111111.10111010.11110000.11111100
      val ipArr: Array[Int] = ip.split("[.]").map(s => Integer.parseInt(s))
      var ipNum = 0L
      //原本ipnum为:00000000.00000000.00000000.00000000
      //第一次循环结束ipnum为:00000000.00000000.00000000.10111111
      //第二次循环ipnum左移:00000000.00000000.10111111.00000000
      //第二次循环结束ipnum:00000000.00000000.10111111.10111010
      //最后10111111.10111010.11110000.11111100
      for (i <- ipArr) {
        ipNum = i | (ipNum << 8)
      }
      ipNum
    }
  }

}
