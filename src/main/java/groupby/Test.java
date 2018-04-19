package groupby;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

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
        List<Integer> data = new ArrayList<>();
        data.add(10);
        data.add(1);
        data.add(6);
        data.add(5);
        data.add(3);

        JavaRDD<Integer> originRDD = javaSparkContext.parallelize(data);
        Map map = originRDD.groupBy(x -> {
            if (x % 2 == 0) {
                return "even";
            } else {
                return "odd";
            }
        }).collectAsMap();

        System.out.println(map);
    }
}
