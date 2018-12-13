package nexusetl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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

    sc.stop()
  }
}
