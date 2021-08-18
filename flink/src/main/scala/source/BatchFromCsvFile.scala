package source

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, GroupedDataSet}

/**
 * 读取CSV文件中的批次数据
 */

object BatchFromCsvFile {
  def main(args: Array[String]): Unit = {

    //初始化环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.api.scala._

    //加载数据
    val dataFile: DataSet[(String, Int, String,Int)] = env.readCsvFile[(String, Int, String,Int)](
      filePath = "/Users/lh/Documents/tb_emedia_incremental_roi_fact_0806.csv",
      lineDelimiter = "\n", //分隔行的字符串，默认为换行。
      fieldDelimiter = ",", //分隔单个字段的字符串，默认值为“，”
      lenient = true, //解析器是否应该忽略格式不正确的行。
      ignoreFirstLine = true, //是否应忽略文件中的第一行。
      includedFields = Array(0, 1, 2,3)
    )

    //触发程序执行
    dataFile.print()

  }

}