import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object WordCountScala extends App{

  val filePath = "/Users/shripaddeshpande/Workspace/TweetSourceAnalysis/inputfile.txt";
  val spark = SparkSession.builder
    .appName("wordcount").master("local").getOrCreate
  import spark.implicits._

  val input: Dataset[String] = spark.read.textFile(filePath).as[String]
  val words = input.flatMap(s => s.split(" "))
  val counts = words.filter(value => !value.toString().isEmpty).groupBy('value).count().orderBy('count.desc)
  counts.show()

}
