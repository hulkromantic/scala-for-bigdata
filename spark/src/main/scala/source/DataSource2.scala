package source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DataSource2 {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setAppName("DataSourceTest").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181")

    val fruitTable: TableName = TableName.valueOf("fruit")
    val tableDescr = new HTableDescriptor(fruitTable)
    tableDescr.addFamily(new HColumnDescriptor("info".getBytes))

    val admin = new HBaseAdmin(conf)
    if (admin.tableExists(fruitTable)) {
      admin.disableTable(fruitTable)
      admin.deleteTable(fruitTable)
    }
    admin.createTable(tableDescr)

    def convert(triple: (String, String, String)): (ImmutableBytesWritable, Put) = {
      val put = new Put(Bytes.toBytes(triple._1))
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(triple._2))
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("price"), Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable, put)
    }

    val dataRDD: RDD[(String, String, String)] = sc.parallelize(List(("1", "apple", "11"), ("2", "banana", "12"), ("3", "pear", "13")))
    val targetRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map(convert)
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "fruit")

    //写入数据
    targetRDD.saveAsHadoopDataset(jobConf)
    println("写入数据成功")

    //读取数据
    conf.set(TableInputFormat.INPUT_TABLE, "fruit")
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count: Long = hbaseRDD.count()
    println("hBaseRDD RDD Count:" + count)
    hbaseRDD.foreach {
      case (_, result) =>
        val key: String = Bytes.toString(result.getRow)
        val name: String = Bytes.toString(result.getValue("info".getBytes, "name".getBytes))
        val color: String = Bytes.toString(result.getValue("info".getBytes, "price".getBytes))
        println("Row key:" + key + " Name:" + name + " Color:" + color)
    }
    sc.stop()

  }
}