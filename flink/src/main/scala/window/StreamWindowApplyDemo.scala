package window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 使用apply实现单词统计
 * apply方法可以进行一些自定义处理，通过匿名内部类的方法来实现。当有一些复杂计算时使用。
 */

object StreamWindowApplyDemo {


  def main(args: Array[String]): Unit = {
    /**
     * 实现思路：
     * 1. 获取流处理运行环境
     * 2. 构建socket流数据源，并指定IP地址和端口号
     * 3. 对接收到的数据转换成单词元组
     * 4. 使用 keyBy 进行分流（分组）
     * 5. 使用 timeWinodw 指定窗口的长度（每3秒计算一次）
     * 6. 实现一个WindowFunction匿名内部类
     * 在apply方法中实现聚合计算
     * 使用Collector.collect收集数据
     * 7. 打印输出
     * 8. 启动执行
     * 9. 在Linux中，使用 nc -lk 端口号 监听端口，并发送单词
     */

    //1. 获取流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 构建socket流数据源，并指定IP地址和端口号
    val textDataStream: DataStream[String] = env.socketTextStream("node01", 9999).flatMap((_: String).split(" "))

    //3. 对接收到的数据转换成单词元组
    val wordDataStream: DataStream[(String, Int)] = textDataStream.map((_: String) -> 1)

    //4. 使用 keyBy 进行分流（分组）
    val groupedDataStream: KeyedStream[(String, Int), String] = wordDataStream.keyBy((_: (String, Int))._1)

    //5. 使用 timeWinodw 指定窗口的长度（每3秒计算一次）
    val windowDataStream: WindowedStream[(String, Int), String, TimeWindow] = groupedDataStream.timeWindow(Time.seconds(3))

    //6. 实现一个WindowFunction匿名内部类
    val reduceDatStream: DataStream[(String, Int)] = windowDataStream.apply(new RichWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
      //在apply方法中实现数据的聚合
      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        println("hello world")
        val tuple: (String, Int) = input.reduce((t1: (String, Int), t2: (String, Int)) => {
          (t1._1, t1._2 + t2._2)
        })
        //将要返回的数据收集起来，发送回去
        out.collect(tuple)
      }
    })

    reduceDatStream.print()
    env.execute()
  }

}
