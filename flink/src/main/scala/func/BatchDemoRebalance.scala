package func

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * 对数据集进行再平衡，重分区，消除数据倾斜
 */

object BatchDemoRebalance {
  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //在不使用rebalance的情况下，观察每一个线程执行的任务特点
    //val ds: DataSet[Long] = env.generateSequence(0, 100)
    //val rebalanced: DataSet[Long] = ds.filter((_: Long) > 8)

    //TODO rebalance
    val ds: DataSet[Long] = env.generateSequence(1, 3000)
    val skewed: DataSet[Long] = ds.filter((_: Long) > 780)
    val rebalanced: DataSet[Long] = skewed.rebalance()

    val countsInPartition: DataSet[(Int, Long)] = rebalanced.map(new RichMapFunction[Long, (Int, Long)] {
      def map(in: Long) = {
        //获取并行时子任务的编号getRuntimeContext.getIndexOfThisSubtask
        (getRuntimeContext.getIndexOfThisSubtask, in)
      }
    })
    countsInPartition.print()

  }

}
