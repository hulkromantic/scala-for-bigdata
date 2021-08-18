package source

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration

/**
 * 遍历目录的批次数据
 */

object BatchFromFolder {
  def main(args: Array[String]): Unit = {

    //初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val parameters = new Configuration

    // recursive.file.enumeration 开启递归
    parameters.setBoolean("recursive.file.enumeration", true)

    val result: DataSet[String] = env.readTextFile("D:\\data\\dataN").withParameters(parameters)

    //触发程序执行
    result.print()

  }

}
