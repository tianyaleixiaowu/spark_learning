package intersection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * 返回两个RDD的交集
 * @author wuweifeng wrote on 2018/4/16.
 */
public class TestIntersection {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的reduce操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        List<Integer> one = Arrays.asList(1, 2, 3, 4, 5);
        List<Integer> two = Arrays.asList(1, 6, 7, 8, 9);
        JavaRDD<Integer> oneRDD = javaSparkContext.parallelize(one);
        JavaRDD<Integer> twoRDD = javaSparkContext.parallelize(two);
        List<Integer> results = oneRDD.intersection(twoRDD).collect();
        System.out.println(results);
    }
}
