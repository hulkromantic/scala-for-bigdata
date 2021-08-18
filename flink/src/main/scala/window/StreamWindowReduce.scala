package window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object StreamWindowReduce {
  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 创建SocketSource
    val stream: DataStream[String] = env.socketTextStream("node01", 9999)

    // 对stream进行处理并按key聚合
    val streamKeyBy: KeyedStream[(String, Int), Tuple] = stream.map((item: String) => (item, 1)).keyBy(0)

    // 引入时间窗口
    val streamWindow: WindowedStream[(String, Int), Tuple, TimeWindow] = streamKeyBy.timeWindow(Time.seconds(5))

    // 执行聚合操作
    val streamReduce: DataStream[(String, Int)] = streamWindow.reduce(
      (item1: (String, Int), item2: (String, Int)) => (item1._1, item1._2 + item2._2)

    )

    // 将聚合数据写入文件
    streamReduce.print()

    // 执行程序
    env.execute("TumblingWindow")

  }

}
