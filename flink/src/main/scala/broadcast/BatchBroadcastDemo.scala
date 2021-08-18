package broadcast

import java.util
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.functions.RichMapFunction

/**
 * 需求：
 * 创建一个 学生 数据集，包含以下数据
 * |学生ID | 姓名 |
 * |------|------|
 * List((1, "张三"), (2, "李四"), (3, "王五"))
 *
 * 再创建一个 成绩 数据集，
 * |学生ID | 学科 | 成绩 |
 * |------|------|-----|
 * List( (1, "语文", 50),(2, "数学", 70), (3, "英文", 86))
 *
 * 请通过广播获取到学生姓名，将数据转换为
 * List( ("张三", "语文", 50),("李四", "数学", 70), ("王五", "英文", 86))
 */

object BatchBroadcastDemo {
  def main(args: Array[String]): Unit = {
    /**
     * 1. 获取批处理运行环境
     * 2. 分别创建两个数据集
     * 3. 使用 RichMapFunction 对 成绩 数据集进行map转换
     * 4. 在数据集调用 map 方法后，调用 withBroadcastSet 将 学生 数据集创建广播
     * 5. 实现 RichMapFunction
     * 将成绩数据(学生ID，学科，成绩) -> (学生姓名，学科，成绩)
     * 重写 open 方法中，获取广播数据
     * 导入 scala.collection.JavaConverters._ 隐式转换
     * 将广播数据使用 asScala 转换为Scala集合，再使用toList转换为scala  List 集合
     * 在 map 方法中使用广播进行转换
     * 6. 打印测试
     */

    //1. 获取批处理运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2. 分别创建两个数据集
    //创建学生数据集
    val stuDataSet: DataSet[(Int, String)] = env.fromCollection(
      List((1, "张三"), (2, "李四"), (3, "王五"))
    )

    //创建成绩数据集
    val scoreDataSet: DataSet[(Int, String, Int)] = env.fromCollection(
      List((1, "语文", 50), (2, "数学", 70), (3, "英文", 86))
    )


    //3. 使用 RichMapFunction 对 成绩 数据集进行map转换
    //返回值类型(学生名字，学科名，成绩)
    val result: DataSet[(String, String, Int)] = scoreDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {

      //定义获取学生数据集的集合
      var studentMap: Map[Int, String] = null

      //初始化的时候被执行一次，在对象的生命周期中只被执行一次
      override def open(parameters: Configuration): Unit = {
        //因为获取到的广播变量中的数据类型是java的集合类型，但是我们的代码是scala因此需要将java的集合转换成scala的集合
        //我们这里将list转换成了map对象，之所以能够转换是因为list中的元素是对偶元祖，因此可以转换成kv键值对类型
        //之所以要转换，是因为后面好用，传递一个学生id，可以直接获取到学生的名字

        import scala.collection.JavaConversions._

        val studentList: util.List[(Int, String)] = getRuntimeContext.getBroadcastVariable[(Int, String)]("student")
        studentMap = studentList.toMap
      }

      //要对集合中的每个元素执行map操作，也就是说集合中有多少元素，就被执行多少次
      override def map(value: (Int, String, Int)): (String, String, Int) = {

        //(Int, String, Int)=》（学生id，学科名字，学生成绩）
        //返回值类型(学生名字，学科成名，成绩)
        val stuId: Int = value._1
        val stuName: String = studentMap.getOrElse(stuId, "")

        //(学生名字，学科名，成绩)
        (stuName, value._2, value._3)
      }

    }).withBroadcastSet(stuDataSet, "student")

    result.print()

  }

}
