package func

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * 左外连接,左边的Dataset中的每一个元素，去连接右边的元素
 */

object BatchLeftOuterJoinDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val data1: ListBuffer[(Int, String)] = ListBuffer[Tuple2[Int, String]]()
    data1.append((1, "zhangsan"))
    data1.append((2, "lisi"))
    data1.append((3, "wangwu"))
    data1.append((4, "zhaoliu"))

    val data2: ListBuffer[(Int, String)] = ListBuffer[Tuple2[Int, String]]()
    data2.append((1, "beijing"))
    data2.append((2, "shanghai"))
    data2.append((4, "guangzhou"))

    val text1: DataSet[(Int, String)] = env.fromCollection(data1)
    val text2: DataSet[(Int, String)] = env.fromCollection(data2)

    text1.leftOuterJoin(text2).where(0).equalTo(0).apply((first: (Int, String), second: (Int, String)) => {
      if (second == null) {
        (first._1, first._2, "null")
      } else {
        (first._1, first._2, second._2)
      }
    }).print()

  }

}
