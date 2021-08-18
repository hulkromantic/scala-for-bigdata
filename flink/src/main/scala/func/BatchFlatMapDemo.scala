package func

import org.apache.flink.api.scala.ExecutionEnvironment

import java.io

object BatchFlatMapDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    // 2. 构建本地集合数据源
    val usersData: DataSet[String] = env.fromCollection(List("张三,中国,江西省,南昌市",
      "李四,中国,河北省,石家庄市",
      "Tom,America,NewYork,Manhattan",
    ))
    val usersInfo: DataSet[(String, String)] = usersData.flatMap((text: String) => {
      val field: Array[String] = text.split(",")

      List(
        (field(0), field(1)),
        (field(0), field(1) + field(2)),
        (field(0), field(1) + field(2) + field(3)))

    })
    usersInfo.print()
  }
}
