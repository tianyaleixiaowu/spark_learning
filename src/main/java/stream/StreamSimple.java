package stream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


/**
 * @author wuweifeng wrote on 2018/4/24.
 */
public class StreamSimple {
    public static void main(String[] args) throws InterruptedException {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, new Duration(5000));

        JavaReceiverInputDStream<String> javaRDD = javaStreamingContext.socketTextStream("localhost", 55533,
                StorageLevel
                .MEMORY_AND_DISK
                ());
        JavaDStream<String> javaDStream = javaRDD.map(s -> s);
        javaDStream.foreachRDD(new VoidFunction() {
            @Override
            public void call(Object o) throws Exception {
                JavaRDD javaRDD1 = (JavaRDD)o;
                System.out.println(javaRDD1.collect());
            }
        });
        //javaRDD.print();

        javaStreamingContext.start();

        javaStreamingContext.awaitTermination();
    }
}
