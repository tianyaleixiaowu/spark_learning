package age;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * 求文件中age的平均值
 *
 * @author wuweifeng wrote on 2018/4/23.
 */
public class Test {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> javaRDD = javaSparkContext.textFile("/users/wuwf/age", 20);
        //总行数
        long count = javaRDD.count();
        JavaRDD<Long> valueRDD = javaRDD.map(s -> Long.valueOf(s.split(" ")[1]));
        Long sum = valueRDD.reduce((a, b) -> a + b);
        System.out.println("总和为：" + sum);
        System.out.println(count);
    }
}
