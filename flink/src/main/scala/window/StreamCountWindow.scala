package window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object StreamCountWindow {
  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 创建SocketSource
    val stream = env.socketTextStream("node01", 9999)
    // 对stream进行处理并按key聚合
    val streamKeyBy = stream.map(item => (item.split(" ")(0), item.split(" ")(1).toLong)).keyBy(0)
    // 引入滚动窗口
    // 这里的5指的是5个相同key的元素计算一次
    //val streamWindow = streamKeyBy.countWindow(5)
    // 引入滑动窗口
    val streamWindow = streamKeyBy.countWindow(5,2)
    // 执行聚合操作
    val streamReduce = streamWindow.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    )
    // 将聚合数据写入文件
    streamReduce.print()
    // 执行程序
    env.execute("TumblingWindow")
  }
}
