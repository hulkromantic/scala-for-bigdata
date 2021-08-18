package func

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * 过滤出来以下以 h 开头的单词。
 * "hadoop", "hive", "spark", "flink"
 */

object BatchFilterDemo {

  def main(args: Array[String]): Unit = {

    //获取执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //获取数据
    val textDataSet: DataSet[String] = env.fromCollection(List("hadoop", "hive", "spark", "flink"))

    //数据过滤
    textDataSet.filter((item: String) => item.startsWith("h")).print()

  }

}
