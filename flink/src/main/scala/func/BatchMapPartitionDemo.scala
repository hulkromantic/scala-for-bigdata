package func

import org.apache.flink.api.scala._

/**
 *
 * 使用mapPartition操作，将以下数据
 *
 * "1,张三", "2,李四", "3,王五", "4,赵六"
 *
 * 转换为一个scala的样例类。
 *
 */

object BatchMapPartitionDemo {

  def main(args: Array[String]): Unit = {

    //获取执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //获取数据源
    val textDataSet: DataSet[String] = env.fromCollection(
      List("1,张三", "2,李四", "3,王五", "4,赵六")
    )

    //将数据转换成样例类
    /*todo mapPartition：是一个分区一个分区拿出来的
    好处就是以后我们操作完数据了需要存储到mysql中，这样做的好处就是几个分区拿几个连接，如果用map的话，就是多少条数据拿多少个mysql的连接
     */
    val userDataSet: DataSet[User] = textDataSet.mapPartition((iter: Iterator[String]) => {

      //访问外部资源，初始化资源
      iter.map((ele: String) => {
        val fields: Array[String] = ele.split(",")
        User(fields(0), fields(1))
      })
    })

    userDataSet.print()

  }

  //定义用户的样例类
  case class User(id: String, name: String)

}
