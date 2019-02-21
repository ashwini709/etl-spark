package nexusetl

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Stream {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "stg.ts.data.reservations")
      .load()

    val df1 = df.writeStream
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()

    println(df1)

//
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "localhost:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "nexus_etl_test_spark",
//      "auto.offset.reset" -> "earliest",
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    )
//
//    val conf: SparkConf = new SparkConf().setAppName("Stream").setMaster("local[2]")
//    val sc: SparkContext = new SparkContext(conf)
//    val ssc = new StreamingContext(sc, Seconds(1))
//    ssc.start()
//
//    val topics = Array("stg.ts.data.reservations")
//    val stream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams)
//    )
//
//    stream.map(record => println(record.key, record.value))

//    sc.stop()
  }
}