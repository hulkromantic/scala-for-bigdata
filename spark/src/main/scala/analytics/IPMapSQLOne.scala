package analytics

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object IPMapSQLOne {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    //2.加载ip规则文件
    val ipFile: RDD[String] = sc.textFile("D:\\授课\\190429\\资料\\data\\ip.txt")
    //3.获取ip起始范围(2)、结束范围(3)、城市信息(4,5,6,7,8)、经度(13)、维度(14)
    val lineArr: RDD[Array[String]] = ipFile.map(_.split("\\|"))
    //RDD[(ip起始值, ip结束值, 城市信息, 经度, 维度)]
    val ipRuleRDD: RDD[(String, String, String, String, String)] = lineArr.map(x=>(x(2),x(3),x(4)+""+x(5)+""+x(6)+""+x(7)+""+x(8),x(13),x(14)))
    val ipRulesArr: Array[(String, String, String, String, String)] = ipRuleRDD.collect()
    //把Ip规则作为广播变量发送到各个Executor,方便一个Excutor中的多个Task共享
    val ipBroadcast: Broadcast[Array[(String, String, String, String, String)]] = sc.broadcast(ipRulesArr)
    import spark.implicits._
    //5.加载日志文件
    val logFile: RDD[String] = sc.textFile("D:\\授课\\190429\\资料\\data\\20190121000132.394251.http.format")
    //6.获取日志中的ip并转为数字最后再将RDD转换成DF
    val ipDF: DataFrame = logFile.map(_.split("\\|")).map(arr=>IPUtils.ipToLong(arr(1))).toDF("ipnum")
    //ipDF.show(10)
    /*
    +----------+
    |     ipnum|
    +----------+
    |2111136891|
    |1969608581|
    |1969610308|
    |1937253494|
    |2076524791|
    |3728161200|
    |2076525149|
    |1937247389|
    |1937246192|
    |1969609713|
    +----------+
     */
    //7.注册表t_rules、t_ips
    ipDF.createOrReplaceTempView("t_ips")
    //8.自定义UDF函数,传入ipnum返回cityinfo
    spark.udf.register("ipnumToCityinfo",(ipnum:Long)=>{
      val ipRules: Array[(String, String, String, String, String)] = ipBroadcast.value
      val index: Int = IPUtils.binarySerarch(ipnum,ipRules)
      val t: (String, String, String, String, String) = ipRules(index)
      t._3 + t._4 + t._5
    })
    //9.关联查询
    val sql:String =
      """
        |select ipnumToCityinfo(ipnum) cityinfo,count(*) counts
        |from t_ips
        |group by cityinfo
        |order by counts desc
      """.stripMargin
    spark.sql(sql).show(truncate = false)
    /*
+----------------------------+------+
    |cityinfo                    |counts|
+----------------------------+------+
    |亚洲中国陕西西安108.94802434.263161 |1824  |
    |亚洲中国北京北京116.40528539.904989 |1535  |
    |亚洲中国重庆重庆106.50496229.533155 |400   |
    |亚洲中国河北石家庄114.50246138.045474|383   |
    |亚洲中国重庆重庆江北106.5743429.60658 |177   |
    |亚洲中国云南昆明102.71225125.040609 |126   |
    |亚洲中国重庆重庆九龙坡106.5110729.50197|91    |
    |亚洲中国重庆重庆武隆107.760129.32548|85    |
    |亚洲中国重庆重庆涪陵107.3900729.70292 |47    |
    |亚洲中国重庆重庆合川106.2763329.97227 |36    |
    |亚洲中国重庆重庆长寿107.0816629.85359 |29    |
    |亚洲中国重庆重庆南岸106.5634729.52311 |3     |
+----------------------------+------+
     */

  }
  object IPUtils{
    def binarySerarch(ipNum: Long, ipRulesArr: Array[(String, String, String, String, String)]): Int = {
      //二分查找
      var start = 0
      var end: Int = ipRulesArr.length -1
      while(start <= end){
        var middle: Int =  (start + end) /2
        val t = ipRulesArr(middle)
        val startIp: Long = t._1.toLong
        val stopIp: Long = t._2.toLong
        //如果传入的ipnum正好在开始和结束范围之内,则返回该索引
        if (ipNum >= startIp && ipNum <= stopIp){
          return middle
        }else if (ipNum < startIp){
          end = middle -1
        }else if (ipNum > stopIp){
          start = middle + 1
        }
      }
      -1 //如果循环结束没有找到则返回-1
    }

    //方法接收一个String类型的IP如:192.168.100.100,返回一个数字如:3232261220
    def ipToLong(ip:String):Long ={
      //注意:IP个原始面貌:
      //10111111.10111010.11110000.11111100
      val ipArr: Array[Int] = ip.split("[.]").map(s => Integer.parseInt(s))
      var ipnum = 0L
      //原本ipnum为:00000000.00000000.00000000.00000000
      //第一次循环结束ipnum为:00000000.00000000.00000000.10111111
      //第二次循环ipnum左移:00000000.00000000.10111111.00000000
      //第二次循环结束ipnum:00000000.00000000.10111111.10111010
      //最后10111111.10111010.11110000.11111100
      for(i <- ipArr){
        ipnum = i | (ipnum << 8)
      }
      ipnum
    }
  }
}
