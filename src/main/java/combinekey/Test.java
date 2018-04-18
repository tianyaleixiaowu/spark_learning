package combinekey;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
        List<ScoreDetail> data = new ArrayList<>();
        data.add(new ScoreDetail("xiaoming", "Math", 98));
        data.add(new ScoreDetail("xiaoming", "English", 88));
        data.add(new ScoreDetail("wangwu", "Math", 75));
        data.add(new ScoreDetail("wangwu", "English", 78));
        data.add(new ScoreDetail("lihua", "Math", 90));
        data.add(new ScoreDetail("lihua", "English", 80));
        data.add(new ScoreDetail("zhangsan", "Math", 91));
        data.add(new ScoreDetail("zhangsan", "English", 80));

        JavaRDD<ScoreDetail> originRDD = javaSparkContext.parallelize(data);
        //转为name->score的键值对，[{"xiaoming":98, "xiaoming":88}]
        JavaPairRDD<String, Integer> pairRDD = originRDD.mapToPair(scoreDetail -> new Tuple2<>(scoreDetail
                .getStudentName(), scoreDetail.getScore()));

        Map<String, Tuple2> map = pairRDD.combineByKey(
            //第一个是createCombiner，也就是将pairRDD的value作为参数，经过操作，转为另一个value。
            //此处就是将score，转为<score, 1>。那么此时的key-value就变成了"xiaoming":(98, 1)
            new Function<Integer, Tuple2>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer score) throws Exception {
                //(98,1)代表1是计数器，代表已经累加了几个科目
                return new Tuple2<>(score, 1);
            }
        },
            //这3个参数第一个是上一个function的返回值，第二个是最早的pairRDD的value，第三个是该函数的返回值类型
            new Function2<Tuple2, Integer, Tuple2>() {
            //v1就是上一步操作返回的Tuple2，即(98,1)
            @Override
            public Tuple2 call(Tuple2 v1, Integer score) throws Exception {
                Tuple2<Integer, Integer> tuple2 = new Tuple2<>((int) v1._1 + score, (int) v1._2 + 1);
                System.out.println(tuple2);
                return tuple2;
            }
        },
            //前两个Tuple2是不同分区上的，通过上一个函数得到的返回值，即(score1+score2 : 2)
            new Function2<Tuple2, Tuple2, Tuple2>() {
            @Override
            public Tuple2 call(Tuple2 v1, Tuple2 v2) throws Exception {
                return new Tuple2((int)v1._1 + (int)v2._1, (int)v1._2 + (int)v2._2);
            }
        }).collectAsMap();

        System.out.println(map);
    }
}
