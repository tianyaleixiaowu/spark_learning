package sparkcontext.sample;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * 采样
 * @author wuweifeng wrote on 2018/4/24.
 */
public class TestSample {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> javaRDD = javaSparkContext.textFile("/users/wuwf/sparkcontext.age");
        //取10%的数据，随机数种子自己设定，也可以不设定
        JavaRDD<String> sample = javaRDD.sample(false, 0.1, 1234);
        long sampleDataSize = sample.count();
        long rawDataSize = javaRDD.count();
        System.out.println(rawDataSize + " and after the sampling: " + sampleDataSize);

        //取指定数量的随机数据
        List<String> list = javaRDD.takeSample(false, 10);
        System.out.println(list);

        //取排序好的指定数量的数据
        List<String> orderList = javaRDD.takeOrdered(10);
        System.out.println(orderList);
    }
}
