package cartesian;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 返回笛卡尔积，开销很大
 * @author wuweifeng wrote on 2018/4/16.
 */
public class TestCartesian {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的reduce操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        List<Integer> one = Arrays.asList(1, 2, 3);
        List<Integer> two = Arrays.asList(1, 4, 5);
        JavaRDD<Integer> oneRDD = javaSparkContext.parallelize(one);
        JavaRDD<Integer> twoRDD = javaSparkContext.parallelize(two);
        List<Tuple2<Integer, Integer>> results = oneRDD.cartesian(twoRDD).collect();

        System.out.println(results);
    }
}
