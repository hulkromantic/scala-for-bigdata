package stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 简单的流处理的词频统计
 * 编写Flink程序，可以来接收 socket 的单词数据，并进行单词统计。
 */

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    /**
     * 实现思路：
     * 1. 获取流处理运行环境
     * 2. 构建socket流数据源，并指定IP地址和端口号
     * 3. 对接收到的数据转换成单词元组
     * 4. 使用 keyBy 进行分流（分组）
     * 5. 使用 timeWinodw 指定窗口的长度（每5秒计算一次）
     * 6. 使用sum执行累加
     * 7. 打印输出
     * 8. 启动执行
     * 9. 在Linux中，使用 nc -lk 端口号 监听端口，并发送单词
     */

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2. 构建socket流数据源，并指定IP地址和端口号
    val textDataStream: DataStream[String] = env.socketTextStream("localhost", 9999)

    //3. 对接收到的数据转换成单词元组
    val wordDataStream: DataStream[(String, Int)] = textDataStream.flatMap((_: String).split(" ")).map((_: String) -> 1)

    //4. 使用 keyBy 进行分流（分组）
    //在批处理中针对于dataset， 如果分组需要使用groupby
    //在流处理中针对于datastream， 如果分组（分流）使用keyBy
    val groupedDataStream: KeyedStream[(String, Int), Tuple] = wordDataStream.keyBy(0)

    //5. 使用 timeWinodw 指定窗口的长度（每5秒计算一次）
    //spark-》reduceBykeyAndWindow
    val windowDataStream: WindowedStream[(String, Int), Tuple, TimeWindow] = groupedDataStream.timeWindow(Time.seconds(5))

    //6. 使用sum执行累加
    val sumDataStream: DataStream[(String, Int)] = windowDataStream.sum(1)
    sumDataStream.print()
    env.execute()

  }

}
