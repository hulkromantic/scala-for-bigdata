package mysql

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import java.sql.{Connection, DriverManager, PreparedStatement}

object MysqlSink {

  case class Student(id: Int, name: String, addr: String, sex: String)

  def main(args: Array[String]): Unit = {
    //1.创建流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2.准备数据
    val dataStream: DataStream[Student] = env.fromElements(
      Student(8, "xiaoming", "beijing biejing", "female")
      //      Student(6, "daming", "tainjing tianjin", "male "),
      //      Student(7, "daqiang ", "shanghai shanghai", "female")
    )

    //3.将数据写入到自定义的sink中（这里是mysql）
    dataStream.addSink(new StudentSinkToMysql)
    //4.触发流执行
    env.execute()
  }

  class StudentSinkToMysql extends RichSinkFunction[Student] {
    private var connection: Connection = null
    private var ps: PreparedStatement = null

    override def open(parameters: Configuration): Unit = {
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://node03:3306/test"
      val username = "root"
      val password = "root"

      //1:加载驱动
      Class.forName(driver)
      //2：创建连接
      connection = DriverManager.getConnection(url, username, password)
      val sql = "insert into student(id , name , addr , sex) values(?,?,?,?);"
      //3:获得执行语句
      ps = connection.prepareStatement(sql)
    }

    //关闭连接操作
    override def close(): Unit = {
      if (connection != null) {
        connection.close()
      }

      if (ps != null) {
        ps.close()
      }
    }

    //每个元素的插入，都要触发一次invoke，这里主要进行invoke插入
    override def invoke(stu: Student): Unit = {
      try {
        //4.组装数据，执行插入操作
        ps.setInt(1, stu.id)
        ps.setString(2, stu.name)
        ps.setString(3, stu.addr)
        ps.setString(4, stu.sex)
        ps.executeUpdate()
      } catch {
        case e: Exception => println(e.getMessage)
      }
    }
  }

}
