package source

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}

/**
 * 读取文件中的批次数据
 */

object BatchFromHdfsFile {

  def main(args: Array[String]): Unit = {

    //使用readTextFile读取本地文件
    //初始化环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //加载数据
    val datas: DataSet[String] = environment.readTextFile("hdfs://bigdata111:9000/README.txt")

    //导入隐式转换
    import org.apache.flink.api.scala._

    //指定数据的转化
    val flatmap_data: DataSet[String] = datas.flatMap((line: String) => line.split("\\W+"))
    val tuple_data: DataSet[(String, Int)] = flatmap_data.map((line: String) => (line, 1))
    val groupData: GroupedDataSet[(String, Int)] = tuple_data.groupBy((line: (String, Int)) => line._1)
    val result: DataSet[(String, Int)] = groupData.reduce((x: (String, Int), y: (String, Int)) => (x._1, x._2 + y._2))

    //触发程序执行
    result.print()

  }

}
