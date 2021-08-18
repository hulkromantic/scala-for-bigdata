package func

import org.apache.flink.api.scala._

/**
 * 请将以下元组数据，使用 distinct 操作去除重复的单词
 * ("java" , 1) , ("java", 1) ,("scala" , 1)
 * 去重得到
 * ("java", 1), ("scala", 1)
 */

object BatchDistinceDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val textDataSet: DataSet[(String, Int)] = env.fromCollection(List(("java" , 1) , ("java", 1) ,("scala" , 1)))
    textDataSet.distinct(0).print()

  }

}
