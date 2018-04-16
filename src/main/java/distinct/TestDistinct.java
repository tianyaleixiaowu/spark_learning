package distinct;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * 去除重复的元素，不过此方法涉及到混洗，操作开销很大
 * @author wuweifeng wrote on 2018/4/16.
 */
public class TestDistinct {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的reduce操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        List<Integer> data = Arrays.asList(1, 1, 2, 3, 4, 5);
        JavaRDD<Integer> originRDD = javaSparkContext.parallelize(data);
        List<Integer> results = originRDD.distinct().collect();
        System.out.println(results);
    }
}
