package kafka

import java.util
import java.util.Properties
import org.apache.commons.collections.map.HashedMap
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition

object StreamKafkaSource {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //隐式转换
    import org.apache.flink.api.scala._

    //指定消费者主题
    val topic = "test"

    val props = new Properties();
    props.setProperty("bootstrap.servers", "node01:9092");
    props.setProperty("group.id", "test091601");
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    //动态感知kafka主题分区的增加 单位毫秒
    props.setProperty("flink.partition-discovery.interval-millis", "5000");

    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props)

    /**
     * Map<KafkaTopicPartition, Long> Long参数指定的offset位置
     * KafkaTopicPartition构造函数有两个参数，第一个为topic名字，第二个为分区数
     * 获取offset信息，可以用过Kafka自带的kafka-consumer-groups.sh脚本获取
     */
    val offsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]();
    offsets.put(new KafkaTopicPartition(topic, 0), 11111111l);
    offsets.put(new KafkaTopicPartition(topic, 1), 222222l);
    offsets.put(new KafkaTopicPartition(topic, 2), 33333333l);

    /**
     * Flink从topic中最初的数据开始消费
     */
    myConsumer.setStartFromEarliest();

    /**
     * Flink从topic中指定的时间点开始消费，指定时间点之前的数据忽略
     */
    myConsumer.setStartFromTimestamp(1559801580000L);

    /**
     * Flink从topic中指定的offset开始，这个比较复杂，需要手动指定offset
     */
    myConsumer.setStartFromSpecificOffsets(offsets);

    /**
     * Flink从topic中最新的数据开始消费
     */
    myConsumer.setStartFromLatest();

    /**
     * Flink从topic中指定的group上次消费的位置开始消费，所以必须配置group.id参数
     */
    myConsumer.setStartFromGroupOffsets()

    //添加消费源
    val text: DataStream[String] = env.addSource(myConsumer)
    text.print()
    env.execute("StreamKafkaSource")
  }

}
