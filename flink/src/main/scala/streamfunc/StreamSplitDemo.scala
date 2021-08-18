package streamfunc

import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.functions.ProcessFunction

object StreamSplitDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val elements: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6)

    //数据分流
    val splitData: DataStream[Int] = elements.process(new split())
    splitData.getSideOutput(new OutputTag[String]("odd")).print("oddTag")
    splitData.getSideOutput(new OutputTag[String]("even")).print("evenTag")

    env.execute("StreamSplitDemo")

    /**
     * split函数已经过期，flink1.1之后，已经删除
     * //数据分流
     * val split_data: Any = elements.split(
     * (num: Int) => (num % 2) match {
     * case 0 => List("even")
     * case 1 => List("odd")
     * }
     * )
     *
     * //获取分流后的数据
     * val select: DataStream[Int] = split_data.select("even")
     * select.print()
     * env.execute()
     * }
     */

  }

  class split() extends ProcessFunction[Int, Int] {
    lazy val evenTag = new OutputTag[String]("even")
    lazy val oddTag = new OutputTag[String]("odd")

    override def processElement(value: Int, ctx: ProcessFunction[Int, Int]#Context, out: Collector[Int]): Unit = {
      if (value % 2 == 0) {
        ctx.output(evenTag, "even" + value)
      } else {
        ctx.output(oddTag, "odd" + value)
      }
    }
  }

}