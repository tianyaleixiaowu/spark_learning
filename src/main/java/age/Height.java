package age;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * 需要计算出男女人数，男性中的最高和最低身高，以及女性中的最高和最低身高。
 *
 * @author wuweifeng wrote on 2018/4/23.
 */
public class Height {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> javaRDD = javaSparkContext.textFile("/users/wuwf/age", 20);
        JavaRDD<Integer> man = javaRDD.filter(s -> s.contains("M")).map(s -> Integer.valueOf(s.split(" ")[2]));
        JavaRDD<Integer> woman = javaRDD.filter(s -> s.contains("F")).map(s -> Integer.valueOf(s.split(" ")[2]));
        System.out.println("男人数量：" + man.count());
        System.out.println("女人数量：" + woman.count());
        System.out.println("女人身高：" + woman.collect());

        int x = woman.sortBy(s -> s, true, 1).first();
        System.out.println(x);

    }
}
