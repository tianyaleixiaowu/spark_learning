package map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * @author wuweifeng wrote on 2018/4/10.
 */
public class SimpleFlatMap {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的reduce操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        List<String> data = Arrays.asList("hello world", "java spark", "hello spark");
        JavaRDD<String> originRDD = javaSparkContext.parallelize(data);

        //flatmap()是将函数应用于RDD中的每个元素，将返回的迭代器的所有内容构成新的RDD
        //RDD经过map后元素数量不变，经过flatmap后，一个元素可以变成多个元素
        JavaRDD<String> flatMap = originRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        System.out.println(flatMap.collect());
    }
}
