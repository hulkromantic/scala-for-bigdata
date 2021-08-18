package udf

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object UDAFDemo {
  def main(args: Array[String]): Unit = {
    //1.获取sparkSession
    val spark: SparkSession = SparkSession.builder().appName("SparkSQL").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    //2.读取文件
    val employeeDF: DataFrame = spark.read.json("D:\\data\\udaf.json")
    //3.创建临时表
    employeeDF.createOrReplaceTempView("t_employee")
    //4.注册UDAF函数
    spark.udf.register("myavg", new MyUDAF)
    //5.使用自定义UDAF函数
    spark.sql("select myavg(salary) from t_employee").show()
    //6.使用内置的avg函数
    spark.sql("select avg(salary) from t_employee").show()
  }


  class MyUDAF extends UserDefinedAggregateFunction {
    //输入的数据类型的schema
    override def inputSchema: StructType = {
      StructType(StructField("input", LongType) :: Nil)
    }

    //缓冲区数据类型schema，就是转换之后的数据的schema
    override def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("total", LongType) :: Nil)
    }

    //返回值的数据类型
    override def dataType: DataType = {
      DoubleType
    }

    //确定是否相同的输入会有相同的输出
    override def deterministic: Boolean = {
      true
    }

    //初始化内部数据结构
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    //更新数据内部结构,区内计算
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      //所有的金额相加
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      //一共有多少条数据
      buffer(1) = buffer.getLong(1) + 1
    }

    //来自不同分区的数据进行合并,全局合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    //计算输出数据值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0).toDouble / buffer.getLong(1)
    }
  }

}