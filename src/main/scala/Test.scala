import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestMainObject extends App{
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/Users/shripaddeshpande/Workspace/TweetSourceAnalysis/inputfile.txt")
    input.flatMap(line => line.split(" "))
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey   { (x, y) => x + y }

    //    val counts1 = input.flatMap(x => x.split(" ")).countByValue()
    counts.saveAsTextFile("/Users/shripaddeshpande/Workspace/TweetSourceAnalysis/outputFile")
    //
    //    val in = sc.parallelize(List(1, 2, 3, 4))
    //    val result = in.map(x => x * x)
    //    println(result.collect().mkString(","))

//    print("hello, world")
}
