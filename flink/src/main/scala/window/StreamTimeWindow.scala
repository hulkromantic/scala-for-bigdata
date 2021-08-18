package window


import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


object StreamTimeWindow {
  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 创建SocketSource
    val stream: DataStream[String] = env.socketTextStream("node01", 9999)
    // 对stream进行处理并按key聚合
    val streamKeyBy: KeyedStream[(String, Long), Tuple] = stream.map((item: String) => (item.split(" ")(0), item.split(" ")(1).toLong)).keyBy(0)
    // 引入滚动窗口
    //val streamWindow: WindowedStream[(String, Long), Tuple, TimeWindow] = streamKeyBy.timeWindow(Time.seconds(5))
    // 引入滑动窗口
    val streamWindow: WindowedStream[(String, Long), Tuple, TimeWindow] = streamKeyBy.timeWindow(Time.seconds(5), Time.seconds(2))
    // 执行聚合操作
    val streamReduce: DataStream[(String, Long)] = streamWindow.reduce(
      (item1: (String, Long), item2: (String, Long)) => (item1._1, item1._2 + item2._2)
    )

    // 将聚合数据写入文件
    streamReduce.print()
    // 执行程序
    env.execute("TumblingWindow")
  }
}
