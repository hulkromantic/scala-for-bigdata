package redis

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * 将数据写入到redis数据库中
 */
object StreamDataToRedis {
  def main(args: Array[String]): Unit = {
    //获取socket端口号
    val port = 9999
    //获取Flink运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //链接socket获取输入数据
    val text: DataStream[String] = env.socketTextStream("node01", port, '\n')

    //注意：必须要添加这一行隐式转行，否则下面的flatmap方法执行会报错
    import org.apache.flink.api.scala._
    val l_wordsData: DataStream[(String, String)] = text.map((line: String) => ("words_scala", line))
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("node01").setPort(6379).build()
    val redisSink = new RedisSink[Tuple2[String, String]](conf, new MyRedisMapper)
    l_wordsData.addSink(redisSink)

    //执行任务
    env.execute("Socket window count");
  }

  class MyRedisMapper extends RedisMapper[Tuple2[String, String]] {
    override def getKeyFromData(data: (String, String)): String = {
      data._1
    }

    override def getValueFromData(data: (String, String)): String = {
      data._2
    }

    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.LPUSH)
    }
  }

}
