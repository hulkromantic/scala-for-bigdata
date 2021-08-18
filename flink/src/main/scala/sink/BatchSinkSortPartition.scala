package sink

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object BatchSinkSortPartition {
  def main(args: Array[String]): Unit = {
    //TODO 使用sortPartition对数据进行排序后再sink到外部系统
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //stu(age,name,height)
    val stu: DataSet[(Int, String, Double)] = env.fromElements(
      (19, "zhangsan", 178.8),
      (17, "lisi", 168.8),
      (18, "wangwu", 184.8),
      (21, "zhaoliu", 164.8)
    )

    //1.以age从小到大升序排列(0->9)
    stu.sortPartition(0, Order.ASCENDING).print

    //2.以name从大到小降序排列(z->a)
    stu.sortPartition(1, Order.ASCENDING).print

    //3.以age升序，height降序排列
    stu.sortPartition(0, Order.ASCENDING).sortPartition(2, Order.DESCENDING).print

    //4.所有字段升序排列
    stu.sortPartition("_", Order.ASCENDING).print

    //5.以Student.name升序
    //5.1准备数据
    case class Student(name: String, age: Int)

    val ds1: DataSet[(Student, Double)] = env.fromElements(
      (Student("zhangsan", 18), 178.5),
      (Student("lisi", 19), 176.5),
      (Student("wangwu", 17), 168.5)
    )

    val ds2: DataSet[(Student, Double)] = ds1.sortPartition("_1.age", Order.ASCENDING).setParallelism(1)

    //5.2写入到hdfs,文本文档
    val outPath1 = "hdfs://hadoop01:9000/Student001.txt"
    ds2.writeAsText(outPath1, WriteMode.OVERWRITE)
    env.execute()

    //5.3写入到hdfs,CSV文档
    val outPath2 = "hdfs://hadoop01:9000/Student002.csv"
    ds2.writeAsCsv(outPath2, "\n", "|||", WriteMode.OVERWRITE)
    env.execute()

  }

}