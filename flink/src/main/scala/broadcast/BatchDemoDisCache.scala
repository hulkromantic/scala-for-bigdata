package broadcast

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import java.io.File
import java.util

/**
 * 分布式缓存\
 */
/**
 * 1)   将 distribute_cache_student 文件上传到HDFS / 目录下
 * 2)   获取批处理运行环境
 * 3)   创建成绩数据集
 * 4)   对 成绩 数据集进行map转换，将（学生ID, 学科, 分数）转换为（学生姓名，学科，分数）
 * a.RichMapFunction 的 open 方法中，获取分布式缓存数据
 * b.在 map 方法中进行转换
 * 5)   实现 open 方法
 * a.使用 getRuntimeContext.getDistributedCache.getFile 获取分布式缓存文件
 * b.使用 Scala.fromFile 读取文件，并获取行
 * c.将文本转换为元组（学生ID，学生姓名），再转换为List
 * 6)   实现 map 方法
 * a.从分布式缓存中根据学生ID过滤出来学生
 * b.获取学生姓名
 * c.构建最终结果元组
 * 7)   打印测试
 */
object BatchDemoDisCache {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    //1:注册文件
    env.registerCachedFile("d:\\data\\file\\a.txt", "b.txt")

    //读取数据
    val data: DataSet[String] = env.fromElements("a", "b", "c", "d")
    val result: DataSet[String] = data.map(mapper = new RichMapFunction[String, String] {

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)

        //访问数据
        val myFile: File = getRuntimeContext.getDistributedCache.getFile("b.txt")
        val lines: util.List[String] = FileUtils.readLines(myFile)
        val it: util.Iterator[String] = lines.iterator()
        while (it.hasNext) {
          val line: String = it.next();
          println("line:" + line)
        }

      }

      override def map(value: String) = {
        value
      }
    })
    result.print()
  }

}
