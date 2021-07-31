import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * counter 累加器
 *
 */

object BatchCounterDemo {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val data = env.fromElements("a", "b", "c", "d")
    val res = data.map(new RichMapFunction[String, String] {
      //1：定义累加器
      val numLines = new IntCounter

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //2:注册累加器
        getRuntimeContext.addAccumulator("num-lines", this.numLines)
      }

      var sum = 0;

      override def map(value: String) = {
        //如果并行度为1，使用普通的累加求和即可，但是设置多个并行度，则普通的累加求和结果就不准了
        sum += 1;
        System.out.println("sum：" + sum);
        this.numLines.add(1)
        value
      }
    }).setParallelism(1)
    res.print();
    res.writeAsText("/Users/lh/Desktop/test")
    val jobResult = env.execute("BatchCounterDemo")
    //
    // 3：获取累加器
    val num = jobResult.getAccumulatorResult[Int]("num-lines")
    println("num:" + num)
  }
}
