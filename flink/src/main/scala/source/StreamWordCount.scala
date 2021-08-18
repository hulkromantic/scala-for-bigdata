package source

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    import org.apache.flink.api.scala._

    val textDateStream: DataStream[String] = env.socketTextStream("localhost", 9999)

    val wordDateStream: DataStream[(String, Int)] = textDateStream.flatMap(_.split(" ")).map(_ -> 1)
    val groupDataStream: KeyedStream[(String, Int), Tuple] = wordDateStream.keyBy(0)
    val windowDataStream: WindowedStream[(String, Int), Tuple, TimeWindow] = groupDataStream.
      window(TumblingProcessingTimeWindows.of(Time.seconds(5)))

    val countDataStream: DataStream[(String, Int)] = windowDataStream.sum(1)
    countDataStream.print()
    env.execute("StreamWordCount")
  }
}
