import org.apache.spark.sql.{DataFrame, SparkSession}

object TweetAnalysis extends App{

  val brokers = "localhost:9092"
  val topic = "twitter-topic"

  val spark = SparkSession.builder().master("local").appName("TweetAnalysis").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val data = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", topic).load()

  val query = data.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream.format("console").start()
//  val query = data.writeStream
//    .format("console")
//    .start()

  query.awaitTermination()
//  val a: DataFrame = data.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//  a.printSchema()
//  a.show()


}
