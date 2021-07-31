import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._

/**
 * 需求：
 * 基于以下列表数据来创建数据源，并按照hashPartition进行分区，然后输出到文件。
 * List(1,1,1,1,1,1,1,2,2,2,2,2)
 */

object BatchHashPartitionDemo {
  def main(args: Array[String]): Unit = {
    /**
     * 实现思路：
     * 1. 构建批处理运行环境
     * 2. 设置并行度为 2
     * 3. 使用 fromCollection 构建测试数据集
     * 4. 使用 partitionByHash 按照字符串的hash进行分区
     * 5. 调用 writeAsText 写入文件到 data/parition_output 目录中
     * 6. 打印测试
     */

    //1. 构建批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    /**
     * 设置并行度三种设置方式
     * 1：读取配置文件中的默认并行度设置
     * 2：设置全局的并行度
     * 3：对算子设置并行度
     */

    //2. 设置并行度为 2
    env.setParallelism(2)

    //3. 使用 fromCollection 构建测试数据集
    val textDataSet: DataSet[(Int, String)] = env.fromCollection(
      List((1, "a"), (1, "b"), (1, "c"), (2, "a"), (2, "b"), (3, "a"), (3, "b"), (3, "c"), (4, "a"), (4, "a"), (5, "a"), (5, "a"))
    )
    val partitionDataSet = textDataSet.partitionByHash(x => x._1)
    val result = partitionDataSet.map(new RichMapFunction[(Int, String), (Int, (Int, String))] {
      override def map(value: (Int, String)): (Int, (Int, String)) = {
        (getRuntimeContext.getIndexOfThisSubtask, value)
      }
    })
    //有多少个并行度就有多少个结果文件，可能结果文件中数据是空的
    //partitionDataSet.writeAsText("./data/output/2")
    result.print()
    //env.execute()
  }
}