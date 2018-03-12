import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class SparkWordCount {

    public static void main(String[] args){
        JavaSparkContext sparkContext =new JavaSparkContext(new SparkConf().setAppName("wordcount"));
        JavaRDD<String> rdd1=sparkContext.textFile(args[0]);
        JavaRDD rdd2 = rdd1.flatMap(new FlatMapFunction<String,String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {

                String[] ss = s.split(" ");
                return Arrays.asList(ss);
            }
        });
        JavaPairRDD rdd3 = rdd2.mapToPair(new PairFunction<String,String,Integer>() {
            @Override
            public Tuple2<String,Integer> call(String o) throws Exception {

                return new Tuple2(o,1);
            }
        }).reduceByKey(new Function2<Integer,Integer,Integer>() {

            @Override
            public Integer call(Integer o, Integer o2) throws Exception {
                return o+o2;
            }
        });
        System.out.println(rdd3.collect());
        rdd3.saveAsTextFile("/tmp/spark-tmp/test");



    }




}
