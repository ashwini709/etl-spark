package nexusetl

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentialsProviderChain, EnvironmentVariableCredentialsProvider}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

case class Reservation(shop: String, id: String)
object Verifier {
  private val config = ConfigFactory.load()
  private val url = config.getString("redshift.url")
  private val iam_role = config.getString("redshift.aws-iam-role")

  val conf: SparkConf = new SparkConf().setAppName("Verifier").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]) {
    val spark = new SparkSession.Builder().getOrCreate()

    import spark.implicits._
    val shopReservationDF = spark.sparkContext.textFile("create_f.txt").map { s =>
      val a = s.split(",")
      val f = a(0).split(" ")
      (f(4), f(2))
    }.map(attr ⇒ Reservation(attr._1, attr._2)).toDF()

    println(shopReservationDF.printSchema())
    shopReservationDF.createOrReplaceTempView("shop_reservations")

    val awsCredentials = new AWSCredentialsProviderChain(new
      EnvironmentVariableCredentialsProvider(), new
      ProfileCredentialsProvider())

    sc.hadoopConfiguration.set("fs.s3a.access.key", awsCredentials.getCredentials.getAWSAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", awsCredentials.getCredentials.getAWSSecretKey)

    val sales_from_redshift = spark.read
      .format("com.databricks.spark.redshift")
      .option("url", url)
      .option("tempdir", "s3a://kkvesper-nexus-staging/verifier/dev/")
      .option("aws-iam-role", iam_role)
      .option("forward_spark_s3_credentials", true)
      .option("query", "select shop_id, reservation_id from reservations_scala")
      .load()

    sales_from_redshift.createOrReplaceTempView("tmp_reservations")

    val salesFromRedshiftDF = sales_from_redshift.select("shop_id", "reservation_id")

    val joinedDF = shopReservationDF
      .join(salesFromRedshiftDF, shopReservationDF("shop") === salesFromRedshiftDF("shop_id"))
      .join(salesFromRedshiftDF, shopReservationDF("id") === salesFromRedshiftDF("reservation_id"))

    println("=" * 100)
    println(joinedDF)
    println("=" * 100)

    sc.stop()
  }
}
