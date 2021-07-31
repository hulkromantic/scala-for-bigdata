package mysql

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

object MysqlSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[Student] = env.addSource(new SQL_source)
    source.print()
    env.execute()
  }

  class SQL_source extends RichSourceFunction[Student] {
    private var connection: Connection = null
    private var ps: PreparedStatement = null

    override def open(parameters: Configuration): Unit = {
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://node03:3306/test"
      val username = "root"
      val password = "root"
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val sql = "select id , name , addr , sex from student"
      ps = connection.prepareStatement(sql)
    }

    override def close(): Unit = {
      if (connection != null) {
        connection.close()
      }
      if (ps != null) {
        ps.close()
      }
    }

    override def run(sourceContext: SourceContext[Student]): Unit = {
      val queryRequest = ps.executeQuery()
      while (queryRequest.next()) {
        val stuid = queryRequest.getInt("id")
        val stuname = queryRequest.getString("name")
        val stuaddr = queryRequest.getString("addr")
        val stusex = queryRequest.getString("sex")
        val stu = new Student(stuid, stuname, stuaddr, stusex)
        sourceContext.collect(stu)
      }
    }

    override def cancel(): Unit = {}
  }

  case class Student(stuid: Int, stuname: String, stuaddr: String, stusex: String) {
    override def toString: String = {
      "stuid:" + stuid + "  stuname:" + stuname + "   stuaddr:" + stuaddr + "   stusex:" + stusex
    }
  }

}
