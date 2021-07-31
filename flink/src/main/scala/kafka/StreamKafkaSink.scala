package kafka


import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.api.scala._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper


object StreamKafkaSink {
  val sinkTopic = "test"

  //样例类
  case class Student(id: Int, name: String, addr: String, sex: String)

  val mapper: ObjectMapper = new ObjectMapper()

  //将对象转换成字符串
  def toJsonString(T: Object): String = {
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(T)
  }

  def main(args: Array[String]): Unit = {
    //1.创建流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2.准备数据
    val dataStream: DataStream[Student] = env.fromElements(
      Student(8, "xiaoming", "beijing biejing", "female")
    )

    //将student转换成字符串
    val studentStream: DataStream[String] = dataStream.map(student =>
      toJsonString(student) // 这里需要显示SerializerFeature中的某一个，否则会报同时匹配两个方法的错误
    )

    //studentStream.print()
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "node01:9092")
    val myProducer = new FlinkKafkaProducer[String](sinkTopic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), prop)
    studentStream.addSink(myProducer)
    studentStream.print()
    env.execute("Flink add sink")
  }

}
