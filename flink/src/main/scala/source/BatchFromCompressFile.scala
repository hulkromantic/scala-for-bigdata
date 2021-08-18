package source

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * 读取压缩文件的数据
 */

object BatchFromCompressFile {
  def main(args: Array[String]): Unit = {

    //初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //todo 注意：读取压缩文件，不能并行处理，因此加载解压的时间会稍微有点长。
    //加载数据
    val result: DataSet[String] = env.readTextFile("D:\\BaiduNetdiskDownload\\hbase-1.3.1-bin.tar.gz")

    //触发程序执行
    result.print()

  }

}
