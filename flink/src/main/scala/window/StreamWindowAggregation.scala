package window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object StreamWindowAggregation {
  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 创建SocketSource
    val stream: DataStream[String] = env.socketTextStream("node01", 9999)

    // 对stream进行处理并按key聚合
    val streamKeyBy: KeyedStream[(String, String), Tuple] = stream.map((item: String) => (item.split(" ")(0), item.split(" ")(1))).keyBy(0)

    // 引入滚动窗口
    val streamWindow: WindowedStream[(String, String), Tuple, TimeWindow] = streamKeyBy.timeWindow(Time.seconds(5))

    // 执行聚合操作
    val streamMax: DataStream[(String, String)] = streamWindow.max(1)

    // 将聚合数据写入文件
    streamMax.print()

    // 执行程序
    env.execute("TumblingWindow")

  }

}
