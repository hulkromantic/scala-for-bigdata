import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * 对数据集进行再平衡，重分区，消除数据倾斜
 */

object BatchDemoRebalance {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds: DataSet[Long] = env.generateSequence(0, 50)
    val skewed: DataSet[Long] = ds.filter(_ > 10)
    //todo rebalance
    val rebalanced: DataSet[Long] = skewed.rebalance()
    val countsInPartition = rebalanced.map(new RichMapFunction[Long, (Int, Long)] {
      def map(in: Long) = {
        //获取并行时子任务的编号getRuntimeContext.getIndexOfThisSubtask
        (getRuntimeContext.getIndexOfThisSubtask, in)
      }
    })
    countsInPartition.print()
    //rebalanced.print()
  }
}