package sortbykey;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 用于对pairRDD按照key进行排序
 * @author wuweifeng wrote on 2018/4/18.
 */
public class Test {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的reduce操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        List<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("A", 10));
        data.add(new Tuple2<>("B", 1));
        data.add(new Tuple2<>("A", 6));
        data.add(new Tuple2<>("C", 5));
        data.add(new Tuple2<>("B", 3));

        JavaPairRDD<String, Integer> originRDD = javaSparkContext.parallelizePairs(data);
        //true为升序，false为倒序
        System.out.println(originRDD.sortByKey(true).collect());
        System.out.println(originRDD.sortByKey(false).collect());
    }
}
