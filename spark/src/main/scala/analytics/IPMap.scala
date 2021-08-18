package analytics

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}

object IPMap {
  def main(args: Array[String]): Unit = {
    //1.创建SparkContext
    val conf: SparkConf = new SparkConf().setAppName("ip").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //2.加载ip规则文件
    val ipFile: RDD[String] = sc.textFile("/Users/lh/ip.txt",2)

    //3.获取ip起始范围(2)、结束范围(3)、城市信息(4,5,6,7,8)、经度(13)、维度(14)
    val lineArr: RDD[Array[String]] = ipFile.map((_: String).split("\\|"))
    //RDD[(ip起始值, ip结束值, 城市信息, 经度, 维度)]
    val ipRuleRDD: RDD[(String, String, String, String, String)] = lineArr.map((x: Array[String]) =>
      (x(2), x(3), x(4) + "" + x(5) + "" + x(6) + "" + x(7) + "" + x(8), x(13), x(14)))
    val ipRules: Array[(String, String, String, String, String)] = ipRuleRDD.collect()
    //注意:二分查找是去有序数组中查找指定元素的索引
    //注意:ipRules后续会被各个Task使用多次,为了避免多次传输,可以把它广播到各个Executor
    val ipRulesBroadcast: Broadcast[Array[(String, String, String, String, String)]] = sc.broadcast(ipRules)
    //4.加载日志文件
    val logFile: RDD[String] = sc.textFile("/Users/lh/20190121000132.394251.http.format")
    //5.将日志的ip分割出来
    val ipRDD: RDD[String] = logFile.map((_: String).split("\\|")).map((_: Array[String]) (1))
    //6.将ip转为数字,并使用二分查找去ipRules中查找出相应的城市信息,记为((城市,经度,纬度),1)
    val cityInfoAndOne: RDD[((String, String, String), Int)] = ipRDD.map((ip: String) => {
      val ipRulesArr: Array[(String, String, String, String, String)] = ipRulesBroadcast.value
      //6.1.ip转数字
      val ipNum: Long = IPUtils.ipToLong(ip)
      //6.2.去ipRules中二分查找出索引
      val index: Int = IPUtils.binarySerarch(ipNum, ipRulesArr)
      //6.3.根据索引去ipRules获取城市,经度,纬度
      val t = ipRulesArr(index)
      //6.4.将(城市,经度,纬度)记为1,即返回((城市,经度,纬度),1)
      ((t._3, t._4, t._5), 1)
    })
    //7.将((城市,经度,维度),1)进行聚合得出统计结果
    //RDD[((城市,经度,维度), count)]
    val cityInfoAndCount: RDD[((String, String, String), Int)] = cityInfoAndOne.reduceByKey((_: Int) + (_: Int))
    cityInfoAndCount.collect().foreach(println)
    //8.将结果数据写入到MySQL中

    val result: RDD[(String, String, String, Int)] = cityInfoAndCount.map((t: ((String, String, String), Int)) => (t._1._1, t._1._2, t._1._3, t._2))
    //result.foreachPartition(iter => IPUtils.save(iter))
    //注意:函数式编程的思想是行为参数化
    result.foreachPartition(IPUtils.save)

    //println(IPUtils.ipToLong("192.168.100.100"))
  }

  object IPUtils {
    def binarySerarch(ipNum: Long, ipRulesArr: Array[(String, String, String, String, String)]): Int = {
      //二分查找
      var start = 0
      var end: Int = ipRulesArr.length - 1
      while (start <= end) {
        var middle: Int = (start + end) / 2
        val t: (String, String, String, String, String) = ipRulesArr(middle)
        val startIp: Long = t._1.toLong
        val stopIp: Long = t._2.toLong
        //如果传入的ipnum正好在开始和结束范围之内,则返回该索引
        if (ipNum >= startIp && ipNum <= stopIp) {
          return middle
        } else if (ipNum < startIp) {
          end = middle - 1
        } else if (ipNum > stopIp) {
          start = middle + 1
        }
      }
      -1 //如果循环结束没有找到则返回-1
    }

    //方法接收一个String类型的IP如:192.168.100.100,返回一个数字如:3232261220
    def ipToLong(ip: String): Long = {
      //注意:IP个原始面貌:
      //10111111.10111010.11110000.11111100
      val ipArr: Array[Int] = ip.split("[.]").map((s: String) => Integer.parseInt(s))
      var ipnum = 0L
      //原本ipnum为:00000000.00000000.00000000.00000000
      //第一次循环结束ipnum为:00000000.00000000.00000000.10111111
      //第二次循环ipnum左移:00000000.00000000.10111111.00000000
      //第二次循环结束ipnum:00000000.00000000.10111111.10111010
      //最后10111111.10111010.11110000.11111100
      for (i <- ipArr) {
        ipnum = i | (ipnum << 8)
      }
      ipnum
    }

    def save(iter: Iterator[(String, String, String, Int)]): Unit = {
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/bigdata", "root", "root")
      val sql: String = "INSERT INTO `iplocaltion` (`id`, `city`, `longitude`, `latitude`, `total_count`) VALUES (NULL, ?, ?, ?, ?);"
      val preparedStatement: PreparedStatement = conn.prepareStatement(sql)
      try {
        for (i <- iter) {
          preparedStatement.setString(1, i._1)
          preparedStatement.setString(2, i._2)
          preparedStatement.setString(3, i._3)
          preparedStatement.setLong(4, i._4)
          preparedStatement.executeUpdate()
        }
      } catch {
        case e: Exception => println(e)
      } finally {
        if (conn != null) {
          conn.close()
        }
        if (preparedStatement != null) {
          preparedStatement.close()
        }
      }
    }
  }

}
