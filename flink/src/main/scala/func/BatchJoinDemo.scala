package func

import org.apache.flink.api.scala._

/**
 * 使用join可以将两个DataSet连接起来
 */

object BatchJoinDemo {

  case class Subject(id: Int, name: String)

  case class Score(id: Int, stuName: String, subId: Int, score: Double)

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val subjectDataSet: DataSet[Subject] = env.readCsvFile[Subject]("/data/input/subject.csv")
    val scoreDataSet: DataSet[Score] = env.readCsvFile[Score]("/data/input/score.csv")

    //join的替代方案：broadcast
    val joinDataSet: JoinDataSet[Score, Subject] = scoreDataSet.join(subjectDataSet).where(_.subId).equalTo(_.id)
    joinDataSet.print()

  }

}
