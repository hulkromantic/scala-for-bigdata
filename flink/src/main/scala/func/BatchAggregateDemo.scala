package func

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._

/**
 * 请将以下元组数据，使用 aggregate 操作进行单词统计
 * ("java" , 1) , ("java", 1) ,("scala" , 1)
 */

object BatchAggregateDemo {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val textDataSet: DataSet[(String, Int)] = env.fromCollection(List(("java", 1), ("java", 1), ("scala", 1)))
    val grouped: GroupedDataSet[(String, Int)] = textDataSet.groupBy(0)
    val aggDataSet: AggregateDataSet[(String, Int)] = grouped.aggregate(Aggregations.MAX, 1)
    aggDataSet.print()
  }

}
