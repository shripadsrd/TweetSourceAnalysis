import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class WordCount {
    public static void main(String args[]) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("wordcount").master("local").getOrCreate();

        List<Row> data = sparkSession.read().text("/Users/shripaddeshpande/Workspace/TweetSourceAnalysis/inputfile.txt").collectAsList();

    }
}
