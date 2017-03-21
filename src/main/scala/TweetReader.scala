import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object TweetReader extends App{
  val sparkConf = new SparkConf().setAppName("Twitter source count").setMaster("local[2]")
  val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
  val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(10))
  val sqlContext = sparkSession.sqlContext

  // Create direct kafka stream with brokers and topics
  val brokers = "localhost:9092"
  val topics = "twitter-topic"
  val topicsSet = topics.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

  val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, topicsSet)

  messages.foreachRDD { rdd =>
    val message: RDD[String] = rdd.map { y => y._2 }
    val df:DataFrame = sqlContext.read.json(message).toDF()
    println("************")
    println(df.show())
    println("************")
  }
  ssc.start()
  ssc.awaitTermination()
}
