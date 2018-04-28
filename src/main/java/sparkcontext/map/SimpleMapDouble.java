package sparkcontext.map;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @author wuweifeng wrote on 2018/4/12.
 */
public class SimpleMapDouble {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的reduce操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        List<Integer> random = Collections.unmodifiableList(
                new Random()
                        .ints(-100, 101).limit(100000000)
                        .boxed()
                        .collect(Collectors.toList())
        );

        //Add more than 100'000'000 of random integers using the Java 8 library for Stream and split them in 10 slides.
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(random, 10);

        //The next step will compute for all the members the power 2 and then generate the JavaDoubleRDD with the statistics.
        JavaDoubleRDD result = rdd.mapToDouble(x -> (double) x * x);

        //Print the statistics for the given DoubleRDD: count, mean stdev, max and min
        System.out.println(result.stats().toString());

    }
}
