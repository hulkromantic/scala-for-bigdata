package state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object TestKeyedState {

  class CountWithKeyedState extends RichFlatMapFunction[(Long, Long), (Long, Long)] {
    /**
     * ValueState状态句柄. 第一个值为count，第二个值为sum。
     */
    private var sum: ValueState[(Long, Long)] = _

    override def flatMap(input: (Long, Long), out: Collector[(Long, Long)]): Unit = {
      // 获取当前状态值
      val tmpCurrentSum: (Long, Long) = sum.value
      //println("tmpCurrentSum: "+tmpCurrentSum)
      // 状态默认值
      val currentSum: (Long, Long) = if (tmpCurrentSum != null) {
        tmpCurrentSum
      } else {
        (0L, 0L)
      }
      println("currentSum:"+currentSum)
      // 更新
      val newSum: (Long, Long) = (currentSum._1 + 1, currentSum._2 + input._2)
      println("newSum:"+newSum)
      // 更新状态值
      sum.update(newSum)
      // 如果count >=3 清空状态值，重新计算
      if (newSum._1 >= 3) {
        val fi: Unit = out.collect((input._1, newSum._2 / newSum._1))
        sum.clear()
      }
    }

    override def open(parameters: Configuration): Unit = {
      sum = getRuntimeContext.getState(
        new ValueStateDescriptor[(Long, Long)]("average", // 状态名称
          TypeInformation.of(new TypeHint[(Long, Long)]() {})) // 状态类型
      )
    }
  }

  def main(args: Array[String]): Unit = {
    //初始化执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //构建数据源
    val inputStream: DataStream[(Long, Long)] = env.fromCollection(
      List(
        (1L, 4L),
        (2L, 3L),
        (3L, 1L),
        (1L, 2L),
        (3L, 2L),
        (1L, 2L),
        (2L, 2L),
        (2L, 9L))
    )
    //执行数据处理
    val keyBy: KeyedStream[(Long, Long), Tuple] = inputStream.keyBy(0)
    keyBy.writeAsCsv("/Users/lh/Desktop/1.csv")
    inputStream.keyBy(0)
      .flatMap(new CountWithKeyedState)
      .setParallelism(1)
      .print

    //运行任务
    env.execute
  }
}
