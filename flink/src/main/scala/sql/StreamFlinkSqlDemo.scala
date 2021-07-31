package sql

import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}

import scala.util.Random

/**
 * 需求：
 * 使用Flink SQL来统计5秒内 用户的 订单总数、订单的最大金额、订单的最小金额
 *
 * timestamp是关键字不能作为字段的名字（关键字不能作为字段名字）
 */

object StreamFlinkSqlDemo {

  /**
   *  1. 获取流处理运行环境
   *     2. 获取Table运行环境
   *     3. 设置处理时间为 EventTime
   *     4. 创建一个订单样例类 Order ，包含四个字段（订单ID、用户ID、订单金额、时间戳）
   *     5. 创建一个自定义数据源
   *     使用for循环生成1000个订单
   *     随机生成订单ID（UUID）
   *     随机生成用户ID（0-2）
   *     随机生成订单金额（0-100）
   *     时间戳为当前系统时间
   *     每隔1秒生成一个订单
   *     6. 添加水印，允许延迟2秒
   *     7. 导入 import org.apache.flink.table.api._ 隐式参数
   *     8. 使用 registerDataStream 注册表，并分别指定字段，还要指定rowtime字段
   *     9. 编写SQL语句统计用户订单总数、最大金额、最小金额
   *     分组时要使用 tumble(时间列, interval '窗口时间' second) 来创建窗口
   *     10. 使用 tableEnv.sqlQuery 执行sql语句
   *     11. 将SQL的执行结果转换成DataStream再打印出来
   *     12. 启动流处理程序
   */

  // 3. 创建一个订单样例类`Order`，包含四个字段（订单ID、用户ID、订单金额、时间戳）
  case class Order(orderId: String, userId: Int, money: Long, createTime: Long)

  def main(args: Array[String]): Unit = {
    // 1. 创建流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 设置处理时间为`EventTime`
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //获取table的运行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 4. 创建一个自定义数据源
    val orderDataStream: DataStream[Order] = env.addSource(new RichSourceFunction[Order] {
      var isRunning = true

      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        // - 随机生成订单ID（UUID）
        // - 随机生成用户ID（0-2）
        // - 随机生成订单金额（0-100）
        // - 时间戳为当前系统时间
        // - 每隔1秒生成一个订单
        for (i <- 0 until 1000 if isRunning) {
          val order: Order = Order(UUID.randomUUID().toString, Random.nextInt(3), Random.nextInt(101),
            System.currentTimeMillis())
          TimeUnit.SECONDS.sleep(1)
          ctx.collect(order)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }

    })


    // 5. 添加水印，允许延迟2秒
    val watermarkDataStream: DataStream[Order] = orderDataStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(2)) {
        override def extractTimestamp(element: Order): Long = {
          val eventTime: Long = element.createTime
          eventTime

        }

      }

    )



    // 6. 导入`import org.apache.flink.table.api._`隐式参数
    // 7. 使用`registerDataStream`注册表，并分别指定字段，还要指定rowtime字段
    tableEnv.registerDataStream("t_order", watermarkDataStream, 'orderId, 'userId, 'money, 'createTime.rowtime)
    // 8. 编写SQL语句统计用户订单总数、最大金额、最小金额
    // - 分组时要使用`tumble(时间列, interval '窗口时间' second)`来创建窗口
    val sql: String =
    """
      |select
      | userId,
      | count(1) as totalCount,
      | max(money) as maxMoney,
      | min(money) as minMoney
      | from
      | t_order
      | group by
      | tumble(createTime, interval '5' second),
      | userId
      """.stripMargin

    // 9. 使用`tableEnv.sqlQuery`执行sql语句
    val table: Table = tableEnv.sqlQuery(sql)

    // 10. 将SQL的执行结果转换成DataStream再打印出来
    table.toRetractStream[Row].print()
    env.execute("StreamSQLApp")

  }

}
