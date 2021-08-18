package kafka

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object StreamWithMyNoParallelSourceDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._
    val text: DataStream[Long] = env.addSource(new MyNoParallelSourceScala).setParallelism(1)
    val mapData: DataStream[Long] = text.map((line: Long) => {
      println("接收到的数据：" + line)
      line
    })
    val sum: DataStream[Long] = mapData.timeWindowAll(Time.seconds(2)).sum(0)
    sum.print().setParallelism(1)
    env.execute("StreamWithMyNoParallelSourceDemo")
  }

  /**
   * 创建自定义并行度为1的source
   * 实现从1开始产生递增数字
   */
  class MyNoParallelSourceScala extends SourceFunction[Long] {
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