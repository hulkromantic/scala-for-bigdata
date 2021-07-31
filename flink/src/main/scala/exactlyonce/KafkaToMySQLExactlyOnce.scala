package exactlyonce

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeutils.base.VoidSerializer
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


object KafkaToMySQLExactlyOnce {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id", args(2))
    props.setProperty("auto.offset.reset", "earliest")

    //kafka的消费者不自动提交偏移量
    props.setProperty("enable.auto.commit", "false")
    //开启Checkpoint
    env.enableCheckpointing(5000L)
    //一旦开启checkpoint，flink会在checkpoint同时，将偏移量更新
    //new FsStateBackend要指定存储系统的协议： scheme (hdfs://, file://, etc)
    env.setStateBackend(new FsStateBackend(args(0)))

    //如果程序被cancle，保留以前做的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //指定以后存储多个checkpoint目录
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //指定重启策略,默认的重启策略是不停的重启
    //程序出现异常是会重启，重启五次，每次延迟5秒，如果超过了5次，程序退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(20, 5000))

    val kafkaConsumer = new FlinkKafkaConsumer[String](
      args(1),
      new SimpleStringSchema,
      props
    )

    //在checkpoint成功的时候提交偏移量
    //可以保证checkpoint是成功的、通过偏移量提交成功
    //kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val lines: DataStream[String] = env.addSource(kafkaConsumer)
    val words: DataStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne: DataStream[(String, Int)] = words.map((_, 1))
    val reduced: DataStream[(String, Int)] = wordAndOne.keyBy(0).sum(1)
    reduced.addSink(new MySqlExactlyOnceSink)
    env.execute()
  }

  class MySqlExactlyOnceSink extends TwoPhaseCommitSinkFunction[(String, Int), MySqlConnectionState, Void](new KryoSerializer(classOf[MySqlConnectionState], new ExecutionConfig), VoidSerializer.INSTANCE) {
    override def beginTransaction(): MySqlConnectionState = {
      val connection: Connection = DriverManager.getConnection("jdbc:mysql://node03:3306/bigdata?characterEncoding=UTF-8", "root", "123456")
      connection.setAutoCommit(false)
      new MySqlConnectionState(connection)
    }

    override def invoke(transaction: MySqlConnectionState, value: (String, Int), context: SinkFunction.Context): Unit = {
      val connection: Connection = transaction.connection
      println("=====> invoke... " + connection)
      val pstm: PreparedStatement = connection.prepareStatement("INSERT INTO t_wordcount (word, counts) VALUES (?, ?) ON DUPLICATE KEY UPDATE counts = ?")
      pstm.setString(1, value._1)
      pstm.setInt(2, value._2)
      pstm.setInt(3, value._2)
      pstm.executeUpdate()
      pstm.close()
    }

    override def preCommit(transaction: MySqlConnectionState): Unit = {
      //transaction.connection
    }

    override def commit(transaction: MySqlConnectionState): Unit = {
      transaction.connection.commit()
      transaction.connection.close()
    }

    override def abort(transaction: MySqlConnectionState): Unit = {
      transaction.connection.rollback()
      transaction.connection.close()
    }
  }

  class MySqlConnectionState(@transient val connection: Connection) {
  }

}
