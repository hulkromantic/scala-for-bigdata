package watermark

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object SlidingEventTimeWindows {
  // 获取执行环境
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  import org.apache.flink.api.scala._

  // 创建SocketSource
  val stream: DataStream[String] = env.socketTextStream("localhost", 11111)

  // 对stream进行处理并按key聚合
  val streamKeyBy: KeyedStream[(String, Int), Tuple] = stream.assignTimestampsAndWatermarks(
    new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
      override def extractTimestamp(element: String): Long = {
        val sysTime: Long = element.split(" ")(0).toLong
        println(sysTime)
        sysTime

      }
    }).map((item: String) => (item.split(" ")(1), 1)).keyBy(0)

  // 引入滚动窗口
  //val streamWindow: WindowedStream[(String, Int), Tuple, TimeWindow] = streamKeyBy.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
  val streamWindow: WindowedStream[(String, Int), Tuple, TimeWindow] = streamKeyBy.timeWindow(Time.seconds(10), Time.seconds(5))
  //val value:  WindowedStream[(String, Int), Tuple, TimeWindow]= streamKeyBy.window(EventTimeSessionWindows.withDynamicGap(Time.seconds(5))

  // 执行聚合操作
  val streamReduce: DataStream[(String, Int)] = streamWindow.reduce(

    (item1: (String, Int), item2: (String, Int)) => (item1._1, item1._2 + item2._2)

  )

  // 将聚合数据写入文件
  streamReduce.print

  // 执行程序
  env.execute("TumblingWindow")
}
