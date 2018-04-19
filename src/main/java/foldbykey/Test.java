package foldbykey;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author wuweifeng wrote on 2018/4/18.
 */
public class Test {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        //spark对普通List的reduce操作
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        List<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("A", 10));
        data.add(new Tuple2<>("A", 20));
        data.add(new Tuple2<>("B", 2));
        data.add(new Tuple2<>("B", 3));
        data.add(new Tuple2<>("C", 5));

        JavaPairRDD<String, Integer> originRDD = javaSparkContext.parallelizePairs(data);
        //初始值为2，那么就会将2先与第一个元素做一次Function操作，将结果再与下一个元素结合
        Map map = originRDD.foldByKey(2, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 * v2;
            }
        }).collectAsMap();

        //{A=400, C=10, B=12}
        System.out.println(map);
    }
}
