package nexusetl

import com.amazonaws.auth._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Verifier {
  val conf: SparkConf = new SparkConf().setAppName("Verifier").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]) {
    val fileName: String = if (args.nonEmpty) args(0) else "*"
    val rdd = sc.textFile(fileName)

    val shopReservationRDD: RDD[(String, String)] = rdd.map { s =>
      val a = s.split(",")
      val f = a(0).split(" ")
      (f(4), f(2))
    }.cache()

    shopReservationRDD.countByKey()

    val provider: AWSCredentialsProvider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials("0LYDLQ3LM2M9SC62IU70", "IfR/qA2lsrKFrcOh/eR2JUr2PuJnVC7EnbQpqiH7")
    )
    val credentials: AWSSessionCredentials = provider.getCredentials.asInstanceOf[AWSSessionCredentials]
    val token = credentials.getSessionToken
    val awsAccessKey = credentials.getAWSAccessKeyId
    val awsSecretKey = credentials.getAWSSecretKey


    val spark = new SparkSession.Builder().getOrCreate()
    val salesDF = spark.read
      .format("com.databricks.spark.redshift")
      .option("url", jdbcURL) //Provide the JDBC URL
      .option("jdbcdriver", "com.amazon.redshift.jdbc42.Driver")
      .option("tempdir", "s3://spark-test")
      .option("temporary_aws_access_key_id", awsAccessKey)
      .option("temporary_aws_secret_access_key", awsSecretKey)
      .option("temporary_aws_session_token", token)
      .option("query", "select shop_id, reservation_id from reservations_scala")
      .load()

    salesDF.createOrReplaceTempView("sales_from_redshift")

    val newSalesDF = spark.sql("SELECT count(*) FROM sales_from_redshift")

    println(newSalesDF)


    sc.stop()
  }
}
