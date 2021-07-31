package sql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * 使用Flink SQL统计用户消费订单的总金额、最大金额、最小金额、订单总数。
 */
object BatchFlinkSqlDemo {

  //3. 创建一个样例类 Order 用来映射数据（订单名、用户名、订单日期、订单金额）
  case class Order(id: Int, userName: String, createTime: String, money: Double)

  def main(args: Array[String]): Unit = {
    /**
     * 实现思路：
     * 1. 获取一个批处理运行环境
     * 2. 获取一个Table运行环境
     * 3. 创建一个样例类 Order 用来映射数据（订单名、用户名、订单日期、订单金额）
     * 4. 基于本地 Order 集合创建一个DataSet source
     * 5. 使用Table运行环境将DataSet注册为一张表
     * 6. 使用SQL语句来操作数据（统计用户消费订单的总金额、最大金额、最小金额、订单总数）
     * 7. 使用TableEnv.toDataSet将Table转换为DataSet
     * 8. 打印测试
     */

    //1. 获取一个批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


    //2. 获取一个Table运行环境
    val tableEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)
    //4. 基于本地 Order 集合创建一个DataSet source
    val orderDataSet: DataSet[Order] = env.fromElements(
      Order(1, "zhangsan", "2018-10-20 15:30", 358.5),
      Order(2, "zhangsan", "2018-10-20 16:30", 131.5),
      Order(3, "lisi", "2018-10-20 16:30", 127.5),
      Order(4, "lisi", "2018-10-20 16:30", 328.5),
      Order(5, "lisi", "2018-10-20 16:30", 432.5),
      Order(6, "zhaoliu", "2018-10-20 22:30", 451.0),
      Order(7, "zhaoliu", "2018-10-20 22:30", 362.0),
      Order(8, "zhaoliu", "2018-10-20 22:30", 364.0),
      Order(9, "zhaoliu", "2018-10-20 22:30", 341.0)
    )

    //5. 使用Table运行环境将DataSet注册为一张表
    tableEnv.registerDataSet("t_order", orderDataSet)

    //6. 使用SQL语句来操作数据（统计用户消费订单的总金额、最大金额、最小金额、订单总数）
    //用户消费订单的总金额、最大金额、最小金额、订单总数。
    val sql: String =
    """
      | select
      |   userName,
      |   sum(money) totalMoney,
      |   max(money) maxMoney,
      |   min(money) minMoney,
      |   count(1) totalCount
      |  from t_order
      |  group by userName
      |""".stripMargin

    //7. 使用TableEnv.toDataSet将Table转换为DataSet
    val table: Table = tableEnv.sqlQuery(sql)
    table.printSchema()
    tableEnv.toDataSet[Row](table).print()
  }
}