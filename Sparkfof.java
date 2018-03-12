import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.LinkedList;

public class Sparkfof {

    public static void main(String[] args){
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("fof"));
        JavaRDD<String> stringJavaRDD = sparkContext.textFile(args[0]);
        JavaRDD<char[]> map = stringJavaRDD.map(new Function<String, char[]>() {
            @Override
            public char[] call(String s) throws Exception {
                return s.replace(" ", "").toCharArray();
            }
        });
        JavaPairRDD<char[], char[]> cartesian = map.cartesian(map);
        JavaRDD<Score> scoreJavaRDD = cartesian.flatMap(new FlatMapFunction<Tuple2<char[], char[]>, Score>() {
            @Override
            public Iterable<Score> call(Tuple2<char[], char[]> tuple2) throws Exception {
                Score aaa = fof.aaa(tuple2._1, tuple2._2);
                LinkedList<Score> scores = new LinkedList<>();
                scores.add(aaa);
                return scores;
            }
        });
        JavaRDD<Score> filter = scoreJavaRDD.filter(new Function<Score, Boolean>() {
            @Override
            public Boolean call(Score score) throws Exception {

                return score != null;
            }
        });

        filter.saveAsTextFile("/tmp/spark-tmp/fof");


    }


}
