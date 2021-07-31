package checkpoint

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.createTypeInformation
import java.util
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


object FlinkEventTimeAPIChkMain {

  //发送数据形式
  case class SEvent(id: Long, name: String, info: String, count: Int)

  class SEventSourceWithChk extends RichSourceFunction[SEvent] {
    private var count = 0L
    private var isRunning = true
    private val alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWZYX0987654321"

    // 任务取消时调用
    override def cancel(): Unit = {
      isRunning = false
    }

    //// source算子的逻辑，即:每秒钟向流图中注入10000个元组
    override def run(sourceContext: SourceContext[SEvent]): Unit = {
      while (isRunning) {
        for (i <- 0 until 10000) {
          sourceContext.collect(SEvent(1, "hello-" + count, alphabet, 1))
          count += 1L
        }
        Thread.sleep(1000)
      }
    }
  }

  /**
   *
   * 该段代码是流图定义代码，具体实现业务流程，另外，代码中窗口的触发时间使 用了event time。
   *
   */
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink-checkpoint/checkpoint/"))
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointInterval(6000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //保留策略:默认情况下，检查点不会被保留，仅用于故障中恢复作业，可以启用外部持久化检查点，同时指定保留策略
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:在作业取消时保留检查点，注意在这种情况下，您必须在取消后手动清理检查点状态
    //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION:当作业被cancel时，删除检查点，检查点状态仅在作业失败时可用
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    // 应用逻辑
    val source: DataStream[SEvent] = env.addSource(new SEventSourceWithChk)
    source.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[SEvent] {
      // 设置watermark
      override def getCurrentWatermark: Watermark = {
        new Watermark(System.currentTimeMillis())
      }

      // 给每个元组打上时间戳
      override def extractTimestamp(t: SEvent, l: Long): Long = {
        System.currentTimeMillis()
      }
    }).keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.seconds(4), Time.seconds(1)))
      .apply(new WindowStatisticWithChk)
      .print()

    env.execute()
  }

  //该数据在算子制作快照时用于保存到目前为止算子记录的数据条数。

  // 用户自定义状态
  class UDFState extends Serializable {
    private var count = 0L

    // 设置用户自定义状态
    def setState(s: Long): Unit = count = s

    // 获取用户自定状态
    def getState: Long = count
  }

  //该段代码是window算子的代码，每当触发计算时统计窗口中元组数量。
  class WindowStatisticWithChk extends WindowFunction[SEvent, Long, Tuple, TimeWindow] with ListCheckpointed[UDFState] {
    private var total = 0L

    // window算子的实现逻辑，即:统计window中元组的数量
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[SEvent], out: Collector[Long]): Unit = {
      var count = 0L
      for (event <- input) {
        count += 1L
      }
      total += count
      out.collect(count)
    }

    // 从自定义快照中恢复状态
    override def restoreState(state: util.List[UDFState]): Unit = {
      val udfState: UDFState = state.get(0)
      total = udfState.getState
    }


    // 制作自定义状态快照
    override def snapshotState(checkpointId: Long, timestamp: Long): util.List[UDFState] = {
      val udfList: util.ArrayList[UDFState] = new util.ArrayList[UDFState]
      val udfState = new UDFState
      udfState.setState(total)
      udfList.add(udfState)
      udfList
    }
  }

}
