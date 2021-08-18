package func

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import scala.collection.mutable
import scala.util.Random

/**
 * 1)   构建批处理运行环境
 * 2)   使用 fromCollection 构建测试数据集
 * 3)   使用 partitionByRange按照字符串的range进行分区
 * 4)   调用 writeAsText 写入文件到 data/rangePartition 目录中
 * 5)   打印测试
 */
object BatchRangePartitionDemo {
  def main(args: Array[String]): Unit = {
    //1. 构建批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //TODO Range-Partition
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

    val collection: DataSet[(Int, Long, String)] = env.fromCollection(Random.shuffle(data))
    val unique: DataSet[(Int, Long, String)] = collection.partitionByRange((x: (Int, Long, String)) => x._1).mapPartition((line: Iterator[(Int, Long, String)]) => line.map {
      x: (Int, Long, String) =>
        (x._1, x._2, x._3)

    })
    unique.writeAsText("/Users/lh/output/rangePartition", WriteMode.OVERWRITE)
    env.execute()

  }

}
