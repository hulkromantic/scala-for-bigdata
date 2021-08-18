package func

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * 左外连接,左边的Dataset中的每一个元素，去连接右边的元素
 */

object BatchFullOuterJoinDemo {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val data1: ListBuffer[(Int, String)] = ListBuffer[Tuple2[Int,String]]()
    data1.append((1,"zhangsan"))
    data1.append((2,"lisi"))
    data1.append((3,"wangwu"))
    data1.append((4,"zhaoliu"))

    val data2: ListBuffer[(Int, String)] = ListBuffer[Tuple2[Int,String]]()
    data2.append((1,"beijing"))
    data2.append((2,"shanghai"))
    data2.append((4,"guangzhou"))

    val text1: DataSet[(Int, String)] = env.fromCollection(data1)
    val text2: DataSet[(Int, String)] = env.fromCollection(data2)


    /**
     * OPTIMIZER_CHOOSES：将选择权交予Flink优化器，相当于没有给提示；
     * BROADCAST_HASH_FIRST：广播第一个输入端，同时基于它构建一个哈希表，而第二个输入端作为探索端，
       选择这种策略的场景是第一个输入端规模很小；
     * BROADCAST_HASH_SECOND：广播第二个输入端并基于它构建哈希表，第一个输入端作为探索端，
       选择这种策略的场景是第二个输入端的规模很小；
     * REPARTITION_HASH_FIRST：该策略会导致两个输入端都会被重分区，但会基于第一个输入端构建哈希表。
      该策略适用于第一个输入端数据量小于第二个输入端的数据量，但这两个输入端的规模仍然很大，优化器也是当没有办法估算大小，
      没有已存在的分区以及排序顺序可被使用时系统默认采用的策略；
     * REPARTITION_HASH_SECOND：该策略会导致两个输入端都会被重分区，但会基于第二个输入端构建哈希表。
      该策略适用于两个输入端的规模都很大，但第二个输入端的数据量小于第一个输入端的情况；
     * REPARTITION_SORT_MERGE：输入端被以流的形式进行连接并合并成排过序的输入。该策略适用于一个或两个输入端都已排过序的情况；
     */

    text1.fullOuterJoin(text2,JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo(0).apply((first: (Int, String), second: (Int, String))=>{
      if(first==null){
        (second._1,"null",second._2)
      }else if(second==null){
        (first._1,first._2,"null")
      }else{
        (first._1,first._2,second._2)
      }

    }).print()
  }

}
