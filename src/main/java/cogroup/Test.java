package cogroup;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;

/**
 *
 * @author wuweifeng wrote on 2018/4/18.
 */
public class Test {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的reduce操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<Tuple2<String, Integer>> rdd1 = javaSparkContext.parallelize(Arrays.asList(
                new Tuple2<>("A", 10),
                new Tuple2<>("B", 20),
                new Tuple2<>("A", 30),
                new Tuple2<>("B", 40)));
        JavaRDD<Tuple2<String, Integer>> rdd2 = javaSparkContext.parallelize(Arrays.asList(
                new Tuple2<>("A", 100),
                new Tuple2<>("B", 200),
                new Tuple2<>("A", 300),
                new Tuple2<>("B", 400)));
        JavaRDD<Tuple2<String, Integer>> rdd3 = javaSparkContext.parallelize(Arrays.asList(
                new Tuple2<>("A", 1000),
                new Tuple2<>("B", 2000),
                new Tuple2<>("A", 3000),
                new Tuple2<>("B", 4000)));

        JavaPairRDD<String, Integer> pairRDD1 = JavaPairRDD.fromJavaRDD(rdd1);
        JavaPairRDD<String, Integer> pairRDD2 = JavaPairRDD.fromJavaRDD(rdd2);
        JavaPairRDD<String, Integer> pairRDD3 = JavaPairRDD.fromJavaRDD(rdd3);
        JavaPairRDD<String, Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>> pairRDD = pairRDD1.cogroup(pairRDD2, pairRDD3);
        System.out.println(pairRDD.collect());
    }
}
