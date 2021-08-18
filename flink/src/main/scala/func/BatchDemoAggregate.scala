package func

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import scala.collection.mutable
import scala.util.Random

/**
 * 在数据集上进行聚合求最值（最大值、最小值）
 * Aggregate只能作用于元组上
 */

object BatchDemoAggregate {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data = new mutable.MutableList[(Int, String, Double)]
    data.+=((1, "yuwen", 89.0))
    data.+=((2, "shuxue", 92.2))
    data.+=((3, "yingyu", 89.99))
    data.+=((4, "wuli", 98.9))
    data.+=((1, "yuwen", 88.88))
    data.+=((1, "wuli", 93.00))
    data.+=((1, "yuwen", 94.3))

    //导入隐式转换
    import org.apache.flink.api.scala._

    //fromCollection将数据转化成DataSet
    val input: DataSet[(Int, String, Double)] = env.fromCollection(Random.shuffle(data))

    input.print()

    println("===========获取指定字段分组后，某个字段的最大值==================")

    val output: AggregateDataSet[(Int, String, Double)] = input.groupBy(1).aggregate(Aggregations.MAX, 2)
    output.print()

    println("===========使用【MinBy】获取指定字段分组后，某个字段的最小值==================")

    //  val input: DataSet[(Int, String, Double)] = env.fromCollection(Random.shuffle(data))
    val output2: DataSet[(Int, String, Double)] = input
      .groupBy(1)
      //求每个学科下的最小分数
      //minBy的参数代表要求哪个字段的最小值
      .minBy(2)
    output2.print()

    println("===========使用【maxBy】获取指定字段分组后，某个字段的最大值==================")

    //  val input: DataSet[(Int, String, Double)] = env.fromCollection(Random.shuffle(data))
    val output3: DataSet[(Int, String, Double)] = input
      .groupBy(1)
      //求每个学科下的最小分数
      //minBy的参数代表要求哪个字段的最小值
      .maxBy(2)
    output3.print()

  }

}
