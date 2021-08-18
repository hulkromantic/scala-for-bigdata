package streamfunc

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object StreamConnectDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    val text1: DataStream[Long] = env.addSource(new MyNoParallelSource)
    val text2: DataStream[Long] = env.addSource(new MyNoParallelSource)
    val text2_str: DataStream[String] = text2.map("str" + (_: Long))
    val connectedStreams: ConnectedStreams[Long, String] = text1.connect(text2_str)
    val result: DataStream[Any] = connectedStreams.map((line1: Long) => {
      line1
    }, (line2: String) => {
      line2
    })
    result.print().setParallelism(1)
    env.execute("StreamConnectDemo")
  }

  /**
   * 创建自定义并行度为1的source
   * 实现从1开始产生递增数字
   */

  class MyNoParallelSource extends SourceFunction[Long] {
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