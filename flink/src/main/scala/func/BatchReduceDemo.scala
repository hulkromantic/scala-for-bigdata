package func

import org.apache.flink.api.scala._

/**
 * 请将以下元组数据，使用 reduce 操作聚合成一个最终结果
 * ("java" , 1) , ("java", 1) ,("java" , 1)
 * 将上传元素数据转换为 ("java",3)
 */

object BatchReduceDemo {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val textDataSet: DataSet[(String, Int)] = env.fromCollection(
      List(("java" , 1) , ("java", 1) ,("java" , 1))

    )

    val reduceDataSet: DataSet[(String, Int)] = textDataSet.reduce((t1: (String, Int), t2: (String, Int))=>(t1._1, t1._2+t2._2))
    reduceDataSet.print()

  }

}
