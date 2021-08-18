package source

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Desc 演示使用Spark操作JDBC-API实现将数据存入到MySQL并读取出来
 */
object JDBCDataSource {

  def main(args: Array[String]): Unit = {
    //1.创建SparkContext
    val config: SparkConf = new SparkConf().setAppName("JDBCDataSourceTest").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")
    //2.插入数据
    val data: RDD[(String, Int)] = sc.parallelize(List(("jack", 18), ("tom", 19), ("rose", 20)))
    //调用foreachPartition针对每一个分区进行操作
    //data.foreachPartition(saveToMySQL)

    //3.读取数据
    def getConn():Connection={
      DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "root")
    }

    val studentRDD: JdbcRDD[(Int, String, Int)] = new JdbcRDD(sc,
      getConn,
      "select * from t_student where id >= ? and id <= ? ",
      4,
      6,
      2,
      rs => {
        val id: Int = rs.getInt("id")
        val name: String = rs.getString("name")
        val age: Int = rs.getInt("age")
        (id, name, age)
      }
    )
    println(studentRDD.collect().toBuffer)
  }
  /*
      CREATE TABLE `t_student` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `name` varchar(255) DEFAULT NULL,
      `age` int(11) DEFAULT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8;
   */
  def saveToMySQL(partitionData:Iterator[(String, Int)] ):Unit = {
    //将数据存入到MySQL
    //获取连接
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "root")
    partitionData.foreach(data=>{
      //将每一条数据存入到MySQL
      val sql = "INSERT INTO `t_student` (`id`, `name`, `age`) VALUES (NULL, ?, ?);"
      val ps: PreparedStatement = conn.prepareStatement(sql)
      ps.setString(1,data._1)
      ps.setInt(2,data._2)
      ps.execute()//preparedStatement.addBatch()
    })

    //ps.executeBatch()
    conn.close()
  }
}
