package func

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object BatchMapDemo {
  def main(args: Array[String]): Unit = {

    //获取执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //获取数据源
    val textDataSet: DataSet[String] = env.fromCollection(
      List("1,张三", "2,李四", "3,王五", "4,赵六")
    )

    //将数据转换成样例类
    val userDataSet: DataSet[User] = textDataSet.map((item: String) => {
      val field: Array[String] = item.split(",")
      User(field(0), field(1))
    })

    userDataSet.print()
  }

  //定义用户的样例类
  case class User(id: String, name: String)

}
