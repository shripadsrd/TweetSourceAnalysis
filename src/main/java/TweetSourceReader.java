import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;


public class TweetSourceReader {
    private static String BROKERS = "localhost:9092";
    private static String TOPIC = "twitter-topic";

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConfiguration = new SparkConf().setAppName("Twitter Source Count").setMaster("local[2]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConfiguration).getOrCreate();
        StreamingContext streamingContext = new StreamingContext(sparkSession.sparkContext(), new Duration(10000));
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(streamingContext);
        SQLContext sqlContext = sparkSession.sqlContext();

        Set<String> topicSet = new HashSet<String>(Arrays.asList(TOPIC.split(",")));
        Map<String, String> kafkaParams = new HashMap();
        kafkaParams.put("metadata.broker.list", BROKERS);

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(javaStreamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicSet);

        //TODO things

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();


    }
}
