package sparkcontext.map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 原RDD中key不变，与新的Value一起组成新的RDD中的元素
 * @author wuweifeng wrote on 2018/4/12.
 */
public class SimpleMapValue {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的map操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> originRDD = javaSparkContext.parallelize(data);
        //转成key-value型
        JavaPairRDD<String, Integer> javaPairRDD = originRDD.mapToPair(a -> new Tuple2<>("key", a));
        //将每个value加1
        List<Tuple2<String, Integer>> list = javaPairRDD.mapValues(a -> a + 1).collect();
        System.out.println(list);
    }
}
