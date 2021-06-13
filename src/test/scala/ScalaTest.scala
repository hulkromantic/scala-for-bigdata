import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ScalaTest {


  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("hello").master("local[*]").getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    //val conf = new SparkConf().setAppName("hello").setMaster("local[*]")
    //val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val lineRDD: RDD[Array[String]] = sc.textFile("/Users/lh/Downloads/spark_test.csv").map(_.split("\\\\001"))

    /**
     *
     * @param str
     * @return
     */
    //def toType(str: String): BigDecimal = {
    //  val deci: BigDecimal = scala.math.BigDecimal(str)
    //  return deci
    //}
    //var a = "11.0"
    //val decimal1: BigDecimal = scala.math.BigDecimal(a)
    val VipRDD: RDD[Vip] = lineRDD.map(x => Vip(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
      x(5).toString, x(6).toString, x(7).toString, x(8).toString, x(9).toString, x(10).toString, x(11).toString, x(12).toString,
      x(13).toString, x(14).toString, x(15).toString, x(16).toString, x(17).toString, x(18).toString, x(19).toString, x(20).toString,
      x(21).toString, x(22).toString, x(23).toString, x(24).toString, x(25).toString, x(26).toString, x(27).toString, x(28).toString,
      x(29).toString, x(30).toString, x(31).toString, x(32).toString, x(33).toString, x(34).toString))
    import sparkSession.implicits._
    val VipDF: DataFrame = VipRDD.toDF()
    //VipDF.show()
    //VipDF.printSchema()
    //注册表
    VipDF.createTempView("vip")

    val newVipDF: DataFrame = sparkSession.sql("select * from vip")
    newVipDF.write.csv("/Users/lh/Downloads/spark_test_new.csv")
    sparkSession.stop()
  }

  case class Vip(ad_date: String, store_id: String, account_name: String, campaign_id: String, campaign_name: String,
                 adgroup_id: String, adgroup_name: String, category_id: String, brand_id: String, impressions: String,
                 clicks: String, cost: String, app_waken_uv: String, cost_per_app_waken_uv: String, app_waken_pv: String,
                 app_waken_rate: String, miniapp_uv: String, app_uv: String, cost_per_app_uv: String, cost_per_miniapp_uv: String,
                 general_uv: String, product_uv: String, special_uv: String, effect: String, book_customer: String,
                 new_customer: String, customer: String, book_sales: String, order_value: String, book_orders: String,
                 order_quantity: String, data_source: String, dw_etl_date: String, dw_batch_id: String, channel: String)
}