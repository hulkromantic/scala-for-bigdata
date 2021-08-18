package streamfunc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * KeyBy算子的使用
 */

object StreamKeyByDemo {
  def main(args: Array[String]): Unit = {
    val port = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        println("No port set, use default port 9000-scala")
      }
        9000
    }

    //获取流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    //获取数据源
    val stream: DataStream[String] = env.socketTextStream("localhost", port, '\n')

    //导入隐式转换
    import org.apache.flink.api.scala._

    //operator操作
    val text: DataStream[(String, Int)] = stream.flatMap((line: String) => line.split("\\s"))
      .map(((w: String) => (w, 1)))

      //TODO 逻辑上将一个流分成不相交的分区，每个分区包含相同键的元素。在内部，这是通过散列分区来实现的
      .keyBy((line: (String, Int)) => line._1)

      //TODO 这里的sum并不是分组去重后的累加值，如果统计去重后累加值，则使用窗口函数
      .sum(1)

    //打印到控制台
    text.print()

    //执行任务
    env.execute(this.getClass.getSimpleName)
  }

}
