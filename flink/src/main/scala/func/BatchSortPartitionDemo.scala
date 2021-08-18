package func

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable
import scala.util.Random

object BatchSortPartitionDemo {
  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  //TODO Sort Partition
  val data = new mutable.MutableList[(Int, Long, String)]
  data.+=((1, 1L, "Hi"))
  data.+=((2, 2L, "Hello"))
  data.+=((3, 2L, "Hello world"))
  data.+=((4, 3L, "Hello world, how are you?"))
  data.+=((5, 3L, "I am fine."))
  data.+=((6, 3L, "Luke Skywalker"))
  data.+=((7, 4L, "Comment#1"))
  data.+=((8, 4L, "Comment#2"))
  data.+=((9, 4L, "Comment#3"))
  data.+=((10, 4L, "Comment#4"))
  data.+=((11, 5L, "Comment#5"))
  data.+=((12, 5L, "Comment#6"))
  data.+=((13, 5L, "Comment#7"))
  data.+=((14, 5L, "Comment#8"))
  data.+=((15, 5L, "Comment#9"))
  data.+=((16, 6L, "Comment#10"))
  data.+=((17, 6L, "Comment#11"))
  data.+=((18, 6L, "Comment#12"))
  data.+=((19, 6L, "Comment#13"))
  data.+=((20, 6L, "Comment#14"))
  data.+=((21, 6L, "Comment#15"))

  import org.apache.flink.api.scala._

  val ds: DataSet[(Int, Long, String)] = env.fromCollection(Random.shuffle(data))
  val result: Seq[(Int, Long, String)] = ds
    .map { x: (Int, Long, String) => x }.setParallelism(2)
    .sortPartition(1, Order.DESCENDING) //第一个参数代表按照哪个字段进行分区
    .mapPartition((line: Iterator[(Int, Long, String)]) => line)
    .collect()
  println(result)
}