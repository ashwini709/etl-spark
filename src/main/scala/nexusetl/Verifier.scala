package nexusetl

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Verifier {
  private val config = ConfigFactory.load()
  private val url = config.getString("redshift.url")
  private val iam_role = config.getString("redshift.aws-iam-role")

  val conf: SparkConf = new SparkConf().setAppName("Verifier").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)


  def main(args: Array[String]) {
//    val fileName: String = if (args.nonEmpty) args(0) else "*"
//    val rdd = sc.textFile(fileName)
//
//    val shopReservationRDD: RDD[(String, String)] = rdd.map { s =>
//      val a = s.split(",")
//      val f = a(0).split(" ")
//      (f(4), f(2))
//    }.cache()
//
//    shopReservationRDD.countByKey()


    val spark = new SparkSession.Builder().getOrCreate()
    val salesDF = spark.read
      .format("com.databricks.spark.redshift")
      .option("url", url) //Provide the JDBC URL
      .option("tempdir", "s3://spark-test/tmp")
      .option("aws-iam-role", iam_role)
      .option("forward_spark_s3_credentials", true)
      .option("query", "select shop_id, reservation_id from reservations_scala")
      .load()

    salesDF.createOrReplaceTempView("sales_from_redshift")

    val newSalesDF = spark.sql("SELECT count(*) FROM sales_from_redshift")

    println(newSalesDF)

    sc.stop()
  }
}
