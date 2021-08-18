package func

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * 通过形成这个数据集和其他数据集的笛卡尔积，创建一个新的数据集。
 */

object BatchDemoCross {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    println("============cross==================")
    cross(env)

    println("============cross2==================")
    cross2(env)

    println("============cross3==================")
    cross3(env)

    println("============crossWithTiny==================")
    crossWithTiny(env)

    println("============crossWithHuge==================")
    crossWithHuge(env)

  }


  /**
   *
   * @param benv
   * 交叉。拿第一个输入的每一个元素和第二个输入的每一个元素进行交叉操作。
   *
   * res71: Seq[((Int, Int, Int), (Int, Int, Int))] = Buffer(
   * ((1,4,7),(10,40,70)), ((2,5,8),(10,40,70)), ((3,6,9),(10,40,70)),
   * ((1,4,7),(20,50,80)), ((2,5,8),(20,50,80)), ((3,6,9),(20,50,80)),
   * ((1,4,7),(30,60,90)), ((2,5,8),(30,60,90)), ((3,6,9),(30,60,90)))
   */

  def cross(benv: ExecutionEnvironment): Unit = {

    //1.定义两个DataSet
    val coords1: DataSet[(Int, Int, Int)] = benv.fromElements((1, 4, 7), (2, 5, 8), (3, 6, 9))
    val coords2: DataSet[(Int, Int, Int)] = benv.fromElements((10, 40, 70), (20, 50, 80), (30, 60, 90))

    //2.交叉两个DataSet[Coord]
    val result1: CrossDataSet[(Int, Int, Int), (Int, Int, Int)] = coords1.cross(coords2)

    //3.显示结果
    println(result1.collect)

  }

  /**
   *
   * @param benv
   * res69: Seq[(Coord, Coord)] = Buffer(
   * (Coord(1,4,7),Coord(10,40,70)), (Coord(2,5,8),Coord(10,40,70)), (Coord(3,6,9),Coord(10,40,70)),
   * (Coord(1,4,7),Coord(20,50,80)), (Coord(2,5,8),Coord(20,50,80)), (Coord(3,6,9),Coord(20,50,80)),
   * (Coord(1,4,7),Coord(30,60,90)), (Coord(2,5,8),Coord(30,60,90)), (Coord(3,6,9),Coord(30,60,90)))
   */

  def cross2(benv: ExecutionEnvironment): Unit = {

    //1.定义 case class
    case class Coord(id: Int, x: Int, y: Int)

    //2.定义两个DataSet[Coord]
    val coords1: DataSet[Coord] = benv.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))
    val coords2: DataSet[Coord] = benv.fromElements(Coord(10, 40, 70), Coord(20, 50, 80), Coord(30, 60, 90))

    //3.交叉两个DataSet[Coord]
    val result1: CrossDataSet[Coord, Coord] = coords1.cross(coords2)

    //4.显示结果
    println(result1.collect)

  }

  /**
   *
   * @param benv
   *
   * res65: Seq[(Int, Int, Int)] = Buffer(
   * (1,1,22), (2,1,24), (3,1,26),
   * (1,2,24), (2,2,26), (3,2,28),
   * (1,3,26), (2,3,28), (3,3,30))
   */

  def cross3(benv: ExecutionEnvironment): Unit = {

    //1.定义 case class
    case class Coord(id: Int, x: Int, y: Int)

    //2.定义两个DataSet[Coord]
    val coords1: DataSet[Coord] = benv.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))
    val coords2: DataSet[Coord] = benv.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))

    //3.交叉两个DataSet[Coord]，使用自定义方法
    val r: DataSet[(Int, Int, Int)] = coords1.cross(coords2) {
      (c1: Coord, c2: Coord) => {
        val dist: Int = (c1.x + c2.x) + (c1.y + c2.y)
        (c1.id, c2.id, dist)
      }
    }

    //4.显示结果
    println(r.collect)

  }


  /**
   * 暗示第二个输入较小的交叉。
   * 拿第一个输入的每一个元素和第二个输入的每一个元素进行交叉操作。
   *
   * @param benv
   * res67: Seq[(Coord, Coord)] = Buffer(
   * (Coord(1,4,7),Coord(10,40,70)), (Coord(1,4,7),Coord(20,50,80)), (Coord(1,4,7),Coord(30,60,90)),
   * (Coord(2,5,8),Coord(10,40,70)), (Coord(2,5,8),Coord(20,50,80)), (Coord(2,5,8),Coord(30,60,90)),
   * (Coord(3,6,9),Coord(10,40,70)), (Coord(3,6,9),Coord(20,50,80)), (Coord(3,6,9),Coord(30,60,90)))
   */

  def crossWithTiny(benv: ExecutionEnvironment): Unit = {

    //1.定义 case class
    case class Coord(id: Int, x: Int, y: Int)

    //2.定义两个DataSet[Coord]
    val coords1: DataSet[Coord] = benv.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))
    val coords2: DataSet[Coord] = benv.fromElements(Coord(10, 40, 70), Coord(20, 50, 80), Coord(30, 60, 90))

    //3.交叉两个DataSet[Coord]，暗示第二个输入较小
    val result1: CrossDataSet[Coord, Coord] = coords1.crossWithTiny(coords2)

    //4.显示结果
    println(result1.collect)

  }

  /**
   *
   * @param benv
   * 暗示第二个输入较大的交叉。
   * 拿第一个输入的每一个元素和第二个输入的每一个元素进行交叉操作。
   * *
   *
   * res68: Seq[(Coord, Coord)] = Buffer(
   * (Coord(1,4,7),Coord(10,40,70)), (Coord(2,5,8),Coord(10,40,70)), (Coord(3,6,9),Coord(10,40,70)),
   * (Coord(1,4,7),Coord(20,50,80)), (Coord(2,5,8),Coord(20,50,80)), (Coord(3,6,9),Coord(20,50,80)),
   * (Coord(1,4,7),Coord(30,60,90)), (Coord(2,5,8),Coord(30,60,90)), (Coord(3,6,9),Coord(30,60,90)))
   */

  def crossWithHuge(benv: ExecutionEnvironment): Unit = {

    //1.定义 case class
    case class Coord(id: Int, x: Int, y: Int)

    //2.定义两个DataSet[Coord]
    val coords1: DataSet[Coord] = benv.fromElements(Coord(1, 4, 7), Coord(2, 5, 8), Coord(3, 6, 9))
    val coords2: DataSet[Coord] = benv.fromElements(Coord(10, 40, 70), Coord(20, 50, 80), Coord(30, 60, 90))

    //3.交叉两个DataSet[Coord]，暗示第二个输入较大
    val result1: CrossDataSet[Coord, Coord] = coords1.crossWithHuge(coords2)

    //4.显示结果
    println(result1.collect)

  }

}
