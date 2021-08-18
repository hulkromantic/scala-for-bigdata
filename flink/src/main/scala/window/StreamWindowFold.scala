package window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object StreamWindowFold {
  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 创建SocketSource
    val stream: DataStream[String] = env.socketTextStream("node01", 9999, '\n', 3)

    // 对stream进行处理并按key聚合
    val streamKeyBy: KeyedStream[(String, Int), Tuple] = stream.map((item: String) => (item, 1)).keyBy(0)

    // 引入滚动窗口
    val streamWindow: WindowedStream[(String, Int), Tuple, TimeWindow] = streamKeyBy.window(TumblingEventTimeWindows.of(Time.seconds(5)))

    // 执行fold操作
    /**
     * flink 1.13 fold已经被弃用
     * val streamFold: Any = streamWindow.fold(100) {
     * (begin, item) =>
     * begin + item._2
     * }
     *
     * // 将聚合数据写入文件
     * streamFold.print()
     */
    // 执行程序
    env.execute("TumblingWindow")

  }

}
