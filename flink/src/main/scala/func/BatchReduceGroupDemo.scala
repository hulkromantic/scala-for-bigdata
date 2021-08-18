package func

import org.apache.flink.api.scala._
/**
 * 请将以下元组数据，下按照单词使用 groupBy 进行分组，再使用 reduceGroup 操作进行单词计数
 * ("java" , 1) , ("java", 1) ,("scala" , 1)
 */

object BatchReduceGroupDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val textDataSet: DataSet[(String, Int)] = env.fromCollection(
      List(("java" , 1) , ("java", 1) ,("java" , 1))
    )

    val reduceGroupDataSet: DataSet[(String, Int)] = textDataSet.reduceGroup((iter: Iterator[(String, Int)]) => {
      iter.reduce((t1: (String, Int), t2: (String, Int)) => {
        (t1._1, t1._2 + t2._2)

      })

    })

    reduceGroupDataSet.print()

  }

}
