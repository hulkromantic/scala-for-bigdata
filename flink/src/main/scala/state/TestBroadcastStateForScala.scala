package state

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import scala.collection.mutable
import java.util.{HashMap, Map, Properties}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.{Tuple2, Tuple4, Tuple6}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.{BroadcastConnectedStream, BroadcastStream, DataStream, DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector


/**
 * 基于Broadcast State 动态更新配置以实现实时过滤数据并增加字段
 */

object TestBroadcastStateForScala {
  def main(args: Array[String]): Unit = {
    //1、初始化配置信息
    //checkpoint配置
    val checkpointDirectory: String = "hdfs://node01:8020/flink-checkpoints"
    val checkpointSecondInterval: Long = 60

    //事件流配置
    val fromKafkaBootstrapServers: String = "node01:9092"
    val fromKafkaGroupID: String = "test"
    val fromKafkaTopic: String = "test"

    //配置流配置
    val fromMysqlHost: String = "node03"
    val fromMysqlPort: Int = 3306
    val fromMysqlDB: String = "test"
    val fromMysqlUser: String = "root"
    val fromMysqlPasswd: String = "root"
    val fromMysqlSecondInterval: Int = 1 //每次读取mysql数据的时间间隔

    //2、配置运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置StateBackend
    env.setStateBackend(new FsStateBackend(checkpointDirectory, true).asInstanceOf[StateBackend])

    //设置Checkpoint
    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointInterval(checkpointSecondInterval * 1000)
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //3、Kafka事件流
    //从Kafka中获取事件数据
    //数据：某个用户在某个时刻浏览或点击了某个商品,如
    //{"userID": "user_3", "eventTime": "2019-08-17 12:19:47", "eventType": "browse", "productID": 1}
    val kafkaProperties: Properties = new Properties
    kafkaProperties.put("bootstrap.servers", fromKafkaBootstrapServers)
    kafkaProperties.put("group.id", fromKafkaGroupID)

    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](fromKafkaTopic, new SimpleStringSchema, kafkaProperties)

    //设置kafka消费数据的位置
    kafkaConsumer.setStartFromLatest()
    val kafkaSource: DataStream[String] = env.addSource(kafkaConsumer).name("KafkaSource").uid("source-id-kafka-source")

    //处理kafka接收到的数据，将字符串转换成元组对象
    val eventStream: SingleOutputStreamOperator[(String, String, String, Int)] = kafkaSource.process(new ProcessFunction[String, (String, String, String, Int)] {
      override def processElement(value: String, ctx: ProcessFunction[String, (String, String, String, Int)]#Context, out: Collector[(String, String, String, Int)]): Unit = {
        try {
          val obj: JSONObject = JSON.parseObject(value)
          val userID: String = obj.getString("userID")
          val eventTime: String = obj.getString("eventTime")
          val eventType: String = obj.getString("eventType")
          val productID: Int = obj.getIntValue("productID")
          out.collect((userID, eventTime, eventType, productID))
        } catch {
          case ex: Exception =>
            println("异常数据:{}", value, ex)
        }
      }
    })


    //4、Mysql配置流
    //自定义Mysql Source，周期性地从Mysql中获取配置，并广播出去
    //数据: 用户ID,用户姓名，用户年龄
    val configStream: DataStreamSource[mutable.Map[String, (String, Int)]] = env.addSource(new MysqlSource(fromMysqlHost, fromMysqlPort, fromMysqlDB, fromMysqlUser, fromMysqlPasswd, fromMysqlSecondInterval))

    /*
      (1) 先建立MapStateDescriptor
      MapStateDescriptor定义了状态的名称、Key和Value的类型。
      这里，MapStateDescriptor中，key是Void类型，value是Map<String, Tuple2<String,Int>>类型。
     */

    //val configDescriptor: MapStateDescriptor[Void, util.Map[String, (String, Int)]] = new MapStateDescriptor[Void, Map[String, (String, Int)]]("config", classOf[Void], classOf[Map[String, (String, Int)]])
    val configDescriptor: MapStateDescriptor[Void, mutable.Map[String, (String, Int)]] = new MapStateDescriptor[Void, mutable.Map[String, (String, Int)]]("config", classOf[Void], classOf[mutable.Map[String, (String, Int)]])
    /*

      (2) 将配置流广播，形成BroadcastStream
     */

    val broadcastConfigStream: BroadcastStream[mutable.Map[String, (String, Int)]] = configStream.broadcast(configDescriptor)

    //5、事件流和广播的配置流连接，形成BroadcastConnectedStream
    val connectedStream: BroadcastConnectedStream[(String, String, String, Int), mutable.Map[String, (String, Int)]] = eventStream.connect(broadcastConfigStream)

    //6、对BroadcastConnectedStream应用process方法，根据配置(规则)处理事件
    val resultStream: SingleOutputStreamOperator[(String, String, String, Int, String, Int)] = connectedStream.process(new CustomBroadcastProcessFunction)

    //7、输出结果
    resultStream.print

    //8、生成JobGraph，并开始执行
    env.execute
  }


  /**
   * 自定义BroadcastProcessFunction
   * 当事件流中的用户ID在配置中出现时，才对该事件处理, 并在事件中补全用户的基础信息
   * Tuple4<String, String, String, Integer>: 第一个流(事件流)的数据类型
   * HashMap<String, Tuple2<String, Integer>>: 第二个流(配置流)的数据类型
   * Tuple6<String, String, String, Integer,String, Integer>: 返回的数据类型
   */

  class CustomBroadcastProcessFunction extends BroadcastProcessFunction[(String, String, String, Int), mutable.Map[String, (String, Int)], (String, String, String, Int, String, Int)] {
    /** 定义MapStateDescriptor */
    val configDescriptor: MapStateDescriptor[Void, mutable.Map[String, (String, Int)]] = new MapStateDescriptor[Void, mutable.Map[String, (String, Int)]]("config", classOf[Void], classOf[mutable.Map[String, (String, Int)]])

    /**
     * 读取状态，并基于状态，处理事件流中的数据
     * 在这里，从上下文中获取状态，基于获取的状态，对事件流中的数据进行处理
     *
     * @param value 事件流中的数据
     * @param ctx   上下文
     * @param out   输出零条或多条数据
     * @throws Exception
     */
    override def processElement(value: (String, String, String, Int), ctx: BroadcastProcessFunction[(String, String, String, Int), mutable.Map[String, (String, Int)], (String, String, String, Int, String, Int)]#ReadOnlyContext, out: Collector[(String, String, String, Int, String, Int)]): Unit = {
      val userID: String = value._1

      //获取状态
      val broadcastState: ReadOnlyBroadcastState[Void, mutable.Map[String, (String, Int)]] = ctx.getBroadcastState(configDescriptor)
      val broadcastStateUserInfo: mutable.Map[String, (String, Int)] = broadcastState.get(null)

      //配置中有此用户，则在该事件中添加用户的userName、userAge字段。
      //配置中没有此用户，则丢弃
      val userInfo: (String, Int) = broadcastStateUserInfo.getOrElse(userID, null)
      if (userInfo != null) out.collect((value._1, value._2, value._3, value._4, userInfo._1, userInfo._2))
    }


    /**
     * 处理广播流中的每一条数据，并更新状态
     *
     * @param value 广播流中的数据
     * @param ctx   上下文
     * @param out   输出零条或多条数据
     * @throws Exception
     */
    override def processBroadcastElement(value: mutable.Map[String, (String, Int)], ctx: BroadcastProcessFunction[(String, String, String, Int), mutable.Map[String, (String, Int)], (String, String, String, Int, String, Int)]#Context, out: Collector[(String, String, String, Int, String, Int)]): Unit = {
      //获取状态
      val broadcastState: BroadcastState[Void, mutable.Map[String, (String, Int)]] = ctx.getBroadcastState(configDescriptor)

      //清空状态
      broadcastState.clear()

      //更新状态
      broadcastState.put(null, value)
    }
  }


  /**
   * 自定义Mysql Source，每隔 secondInterval 秒从Mysql中获取一次配置
   */
  class MysqlSource(var host: String, var port: Integer, var db: String, var user: String, var passwd: String, var secondInterval: Integer) extends RichSourceFunction[scala.collection.mutable.Map[String, (String, Int)]] {
    private var connection: Connection = null
    private var preparedStatement: PreparedStatement = null
    private var isRunning = true

    /**
     * 开始时, 在open()方法中建立连接
     *
     * @param parameters
     * @throws Exception
     */
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      Class.forName("com.mysql.jdbc.Driver")
      connection = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + db + "?useUnicode=true&characterEncoding=UTF-8", user, passwd)
      val sql = "select userID,userName,userAge from user_info"
      preparedStatement = connection.prepareStatement(sql)
    }

    /**
     * 调用run()方法获取数据
     *
     * @param ctx
     */
    override def run(ctx: SourceFunction.SourceContext[scala.collection.mutable.Map[String, (String, Int)]]): Unit = {
      try {
        while (isRunning) {
          val output: mutable.Map[String, (String, Int)] = scala.collection.mutable.Map[String, (String, Int)]()
          val resultSet: ResultSet = preparedStatement.executeQuery
          while (resultSet.next) {
            val userID: String = resultSet.getString("userID")
            val userName: String = resultSet.getString("userName")
            val userAge: Int = resultSet.getInt("userAge")
            output += (userID -> (userName, userAge))
          }
          ctx.collect(output)

          //每隔多少秒执行一次查询
          Thread.sleep(1000 * secondInterval)
        }
      }
      catch {
        case ex: Exception =>
          System.out.println("从Mysql获取配置异常..." + ex)
      }
    }

    /**
     * 取消时，会调用此方法
     */
    override def cancel(): Unit = {
      isRunning = false
    }


    /**
     * 执行完，调用close()方法关系连接，释放资源
     *
     * @throws Exception
     */
    override def close(): Unit = {
      super.close()
      if (connection != null) connection.close()
      if (preparedStatement != null) preparedStatement.close()
    }
  }
}