package source

import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWordCount {
  def main(args: Array[String]): Unit = {

    /**
     * 获得一个 execution environment，
     * 加载/创建初始数据，
     * 导入隐式转换
     * 指定这些数据的转换，
     * 指定将计算结果放在哪里，
     * 触发程序执行
     */

    //获取flink的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.api.scala._

    //加载/创建初始数据
    val text: DataSet[String] = env.fromElements("i love beijing", "i love shanghai")

    //指定这些数据的转换
    val splitWords: DataSet[String] = text.flatMap(_.toLowerCase().split(" "))
    val filterWords: DataSet[String] = splitWords.filter(x => x.nonEmpty)
    val wordAndOne: DataSet[(String, Int)] = filterWords.map(x => (x, 1))
    val groupWords: GroupedDataSet[(String, Int)] = wordAndOne.groupBy(0)
    val sumWords: AggregateDataSet[(String, Int)] = groupWords.sum(1)

    /**
     * 触发程序执行
     * 1: 本地输出
     * sumWords.print()
     */

    sumWords.print()

    /**
     * 触发程序执行
     * 2: 集群执行
     * 1: 指定将结果放到哪里
     * sumWords.writeAsText(args(0))
     * 2：触发程序执行
     * env.execute(this.getClass.getSimpleName)
     */

    //    sumWords.writeAsText(args(0))
    //    env.execute(this.getClass.getSimpleName)

  }

}
