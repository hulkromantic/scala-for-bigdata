import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object ParallelStreamingSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //隐式转换
    import org.apache.flink.api.scala._
    val text: DataStream[Long] = env.addSource(new MyParallelSourceScala).setParallelism(4)
    val mapData: DataStream[Long] = text.map(line => {
      println("接收到的数据：" + line)
      line
    })

    val sum: DataStream[Long] = mapData.timeWindowAll(Time.seconds(2)).sum(0)
    sum.print().setParallelism(1)
    env.execute("ParallelStreamingSource")
  }

  /**
   *
   * 创建自定义并行度为1的source
   * 实现从1开始产生递增数字
   *
   */

  class MyParallelSourceScala extends ParallelSourceFunction[Long] {
    var count = 1L
    var isRunning = true

    override def run(ctx: SourceContext[Long]) = {
      while (isRunning) {
        ctx.collect(count)
        count += 1
        Thread.sleep(1000)
      }
    }

    override def cancel() = {
      isRunning = false
    }
  }

}
