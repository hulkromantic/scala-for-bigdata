package exactlyonce

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, FlinkKafkaProducer011}
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
 * Kafka Producer的容错-Kafka 0.9 and 0.10
 * 如果Flink开启了checkpoint，针对FlinkKafkaProducer09 和FlinkKafkaProducer010 可以提供 at-least-once的语义，还需要配置下面两个参数
 * •setLogFailuresOnly(false)
 * •setFlushOnCheckpoint(true)
 *
 * 注意：建议修改kafka 生产者的重试次数
 * retries【这个参数的值默认是0】
 *
 * Kafka Producer的容错-Kafka 0.11
 * 如果Flink开启了checkpoint，针对FlinkKafkaProducer011 就可以提供 exactly-once的语义
 * 但是需要选择具体的语义
 * •Semantic.NONE
 * •Semantic.AT_LEAST_ONCE【默认】
 * •Semantic.EXACTLY_ONCE
 */

object StreamToKafkaExactlyOnce {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._
    //checkpoint配置
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val text: DataStream[String] = env.socketTextStream("node01", 9001, '\n')
    val topic = "test"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "node01:9092")

    //设置事务超时时间，也可在kafka配置中设置
    prop.setProperty("transaction.timeout.ms", 60000 * 15 + "");

    //使用至少一次语义的形式
    //val myProducer = new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema());
    //使用支持仅一次语义的形式
    val myProducer = new FlinkKafkaProducer[String](topic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), prop,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    text.addSink(myProducer)
    env.execute("StreamToKafkaExactlyOnce")
  }

}