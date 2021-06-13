package stream

import java.sql.{DriverManager, ResultSet}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{OffsetRange, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

object ConsumerFromKafka2 {
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext
    //spark.master should be set as local[n], n > 1
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5)) //5表示5秒中对数据进行切分形成一个RDD
    //准备连接Kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "ConsumerFromKafka2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("spark_kafka")

    //2.使用KafkaUtil连接Kafak获取数据
    //注意:
    //如果MySQL中没有记录offset,则直接连接,从latest开始消费
    //如果MySQL中有记录offset,则应该从该offset处开始消费
    val offsetMap: mutable.Map[TopicPartition, Long] = OffsetUtil.getOffsetMap("ConsumerFromKafka2", "spark_kafka")
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.size > 0) { //有记录offset
      println("MySQL中记录了offset,则从该offset处开始消费")
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent, //位置策略,源码强烈推荐使用该策略,会让Spark的Executor和Kafka的Broker均匀对应
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsetMap)) //消费策略,源码强烈推荐使用该策略
    } else { //没有记录offset
      println("没有记录offset,则直接连接,从latest开始消费")
      // /export/servers/kafka/bin/kafka-console-producer.sh --broker-list node01:9092 --topic  spark_kafka
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent, //位置策略,源码强烈推荐使用该策略,会让Spark的Executor和Kafka的Broker均匀对应
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)) //消费策略,源码强烈推荐使用该策略
    }

    //3.操作数据
    //注意:我们的目标是要自己手动维护偏移量,也就意味着,消费了一小批数据就应该提交一次offset
    //而这一小批数据在DStream的表现形式就是RDD,所以我们需要对DStream中的RDD进行操作
    //而对DStream中的RDD进行操作的API有transform(转换)和foreachRDD(动作)
    recordDStream.foreachRDD(rdd => {
      if (rdd.count() > 0) { //当前这一时间批次有数据
        rdd.foreach(record => println("接收到的Kafk发送过来的数据为:" + record))
        //接收到的Kafk发送过来的数据为:ConsumerRecord(topic = spark_kafka, partition = 1, offset = 6, CreateTime = 1565400670211,
        // checksum = 1551891492, serialized key size = -1, serialized value size = 43, key = null, value = hadoop spark ...)
        //注意:通过打印接收到的消息可以看到,里面有我们需要维护的offset,和要处理的数据
        //接下来可以对数据进行处理....或者使用transform返回和之前一样处理
        //处理数据的代码写完了,就该维护offset了,那么为了方便我们对offset的维护/管理,spark提供了一个类,帮我们封装offset的数据
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (o <- offsetRanges) {
          println(s"topic=${o.topic},partition=${o.partition},fromOffset=${o.fromOffset},untilOffset=${o.untilOffset}")
        }
        //手动提交offset,默认提交到Checkpoint中
        //recordDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        //实际中偏移量可以提交到MySQL/Redis中
        OffsetUtil.saveOffsetRanges("ConsumerFromKafka2", offsetRanges)
      }
    })

    /* val lineDStream: DStream[String] = recordDStream.map(_.value())//_指的是ConsumerRecord
     val wrodDStream: DStream[String] = lineDStream.flatMap(_.split(" ")) //_指的是发过来的value,即一行数据
     val wordAndOneDStream: DStream[(String, Int)] = wrodDStream.map((_,1))
     val result: DStream[(String, Int)] = wordAndOneDStream.reduceByKey(_+_)
     result.print()*/
    ssc.start() //开启
    ssc.awaitTermination() //等待优雅停止
  }

  /*
  手动维护offset的工具类
  首先在MySQL创建如下表
    CREATE TABLE `t_offset` (
      `topic` varchar(255) NOT NULL,
      `partition` int(11) NOT NULL,
      `groupid` varchar(255) NOT NULL,
      `offset` bigint(20) DEFAULT NULL,
      PRIMARY KEY (`topic`,`partition`,`groupid`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
   */
  object OffsetUtil {

    //从数据库读取偏移量
    def getOffsetMap(groupid: String, topic: String) = {
      val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "root")
      val pstmt = connection.prepareStatement("select * from t_offset where groupid=? and topic=?")
      pstmt.setString(1, groupid)
      pstmt.setString(2, topic)
      val rs: ResultSet = pstmt.executeQuery()
      val offsetMap = mutable.Map[TopicPartition, Long]()
      while (rs.next()) {
        offsetMap += new TopicPartition(rs.getString("topic"), rs.getInt("partition")) -> rs.getLong("offset")
      }
      rs.close()
      pstmt.close()
      connection.close()
      offsetMap
    }

    //将偏移量保存到数据库
    def saveOffsetRanges(groupid: String, offsetRange: Array[OffsetRange]) = {
      val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "root")
      //replace into表示之前有就替换,没有就插入
      val pstmt = connection.prepareStatement("replace into t_offset (`topic`, `partition`, `groupid`, `offset`) values(?,?,?,?)")
      for (o <- offsetRange) {
        pstmt.setString(1, o.topic)
        pstmt.setInt(2, o.partition)
        pstmt.setString(3, groupid)
        pstmt.setLong(4, o.untilOffset)
        pstmt.executeUpdate()
      }
      pstmt.close()
      connection.close()
    }
  }

}
