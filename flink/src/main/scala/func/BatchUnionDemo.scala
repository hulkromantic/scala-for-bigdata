package func

import org.apache.flink.api.scala._

/**
 * 将两个DataSet取并集，并不会进行去重。
 */

object BatchUnionDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 2. 使用`fromCollection`创建两个数据源
    val wordDataSet1: DataSet[String] = env.fromCollection(List("hadoop", "hive", "flume"))
    val wordDataSet2: DataSet[String] = env.fromCollection(List("hadoop", "hive", "spark"))

    val wordDataSet3: DataSet[String] = env.fromElements("hadoop")
    val wordDataSet4: DataSet[String] = env.fromElements("hadoop")

    wordDataSet1.union(wordDataSet2).print()
    wordDataSet3.union(wordDataSet4).print()

  }

}
