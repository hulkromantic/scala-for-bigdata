package kafka

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object StreamWithMyRichParallelSourceDemo {
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    val text: DataStream[Long] = env.addSource(new MyRichParallelSourceScala).setParallelism(2)
    val mapData: DataStream[Long] = text.map((line: Long) => {
      println("接收到的数据：" + line)
      line
    })

    //每两秒钟获取一次数据
    val sum: DataStream[Long] = mapData.timeWindowAll(Time.seconds(2)).sum(0)
    sum.print().setParallelism(1)
    env.execute(this.getClass.getSimpleName)

  }


  /**
   *
   * 创建自定义并行度为1的source
   *
   * 实现从1开始产生递增数字
   *
   */

  class MyRichParallelSourceScala extends RichParallelSourceFunction[Long] {
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

    override def open(parameters: Configuration): Unit = super.open(parameters)

    override def close(): Unit = super.close()
  }

}