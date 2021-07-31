package state

import java.util
import java.util.concurrent.TimeUnit
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector

object TestOperatorState {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置checkpoint
    env.enableCheckpointing(60000L)
    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConfig.setMinPauseBetweenCheckpoints(30000L)
    checkpointConfig.setCheckpointTimeout(10000L)
    checkpointConfig.setFailOnCheckpointingErrors(true)
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    //        StateBackend backend=new FsStateBackend(
    //                "hdfs://namenode:40010/flink/checkpoints",
    //                false);
    //        StateBackend backend=new MemoryStateBackend(10*1024*1024,false);
    val backend = new RocksDBStateBackend("hdfs://node01:8020/flink/checkpoints", true)
    env.setStateBackend(backend)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, // number of restart attempts
      Time.of(10, TimeUnit.SECONDS)))

    val inputStream: DataStream[Long] = env.fromElements(1L, 2L, 3L, 4L, 5L, 1L, 3L, 4L, 5L, 6L, 7L, 1L, 4L, 5L, 3L, 9L, 9L, 2L, 1L)
    inputStream.flatMap(new CountWithOperatorState).setParallelism(2).print
    env.execute()
  }

  /**
   *
   * 想知道两次事件1之间，一共发生多少次其他事件，分别是什么事件
   *
   * 事件流：1 2 3 4 5 1 3 4 5 6 7 1 4 5 3 9 9 2 1...
   * 输出：
   * (4,2 3 4 5)
   * (5,3 4 5 6 7)
   * (6,4 5 6 9 9 2)
   */

  class CountWithOperatorState extends RichFlatMapFunction[Long, (Int, String)] with CheckpointedFunction {
    private var checkPointCountList: ListState[Long] = null
    /**
     * 原始状态
     */
    private var listBufferElements: util.ArrayList[Long] = null

    override def flatMap(value: Long, out: Collector[(Int, String)]): Unit = {
      if(value == 1){
        if (listBufferElements.size > 0) {
          val buffer: StringBuffer = new StringBuffer
          import scala.collection.JavaConversions._
          for (item <- listBufferElements) {
            buffer.append(item + " ")
          }
          out.collect((listBufferElements.size, buffer.toString))
          listBufferElements.clear()
        }
      } else listBufferElements.add(value)
    }

    //当发生snapshot时，将operatorCount添加到operatorState中
    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      checkPointCountList.clear()
      import scala.collection.JavaConversions._
      for (item <- listBufferElements) {
        checkPointCountList.add(item)
      }
    }

    //初始化状态数据
    override def initializeState(context: FunctionInitializationContext): Unit = { //定义并获取ListState
      val listStateDescriptor: ListStateDescriptor[Long] = new ListStateDescriptor[Long]("checkPointCountList", TypeInformation.of(new TypeHint[Long]() {}))
      checkPointCountList = context.getOperatorStateStore.getListState(listStateDescriptor)
      //定义在Restored过程中，从operatorState中恢复数据的逻辑
      if (context.isRestored) {
        import scala.collection.JavaConversions._
        for (element <- checkPointCountList.get) {
          listBufferElements.add(element)
        }
      }
    }

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      listBufferElements = new util.ArrayList[Long]
    }
  }
}

