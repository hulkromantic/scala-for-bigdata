package restartstrategy

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._

object FixDelayRestartStrategiesDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //如果想要开启重启策略，就必须开启CheckPoint
    env.enableCheckpointing(5000L)

    //指定状态存储后端,默认就是内存
    //现在指定的是FsStateBackend，支持本地系统、
    //new FsStateBackend要指定存储系统的协议： scheme (hdfs://, file://, etc)
    env.setStateBackend(new FsStateBackend(args(0)))

    //如果程序被cancle，保留以前做的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //指定以后存储多个checkpoint目录
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)

    //指定重启策略,默认的重启策略是不停的重启
    //程序出现异常是会重启，重启五次，每次延迟5秒，如果超过了5次，程序退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 5000))

    val lines: DataStream[String] = env.socketTextStream(args(1), 8888)

    val result: DataStream[(String, Int)] = lines.flatMap((_: String).split(" ").map((word: String) => {
      if (word.equals("zhangsan")) {
        throw new RuntimeException("zhangsan，程序重启！");
      }
      (word, 1)
    })).keyBy(0).sum(1)
    result.print()
    env.execute()

  }

}
