package com.rowspriego.spark.training;

// import java.util.Map;
//
// import org.apache.spark.SparkConf;
// import org.apache.spark.api.java.JavaRDD;
// import org.apache.spark.api.java.JavaSparkContext;
// import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
//
// import com.google.common.collect.ImmutableList;
// import com.google.common.collect.ImmutableMap;
//
/// **
// * Almacenar info en ES desde Spark. Todo muy fácil, es solo la configuración
// * del POM y la configuración del sparkConf
// *
// */
// public class Spark2ES {
//
// public static void main(String[] args) throws InterruptedException {
//
// SparkConf sparkConf = new
// SparkConf().setMaster("local[2]").setAppName("spark2ES")
// .set("hadoop.home.dir",
// "/home/bigdata/hadoop-2.7.3").set("es.index.auto.create", "true")
// .set("es.nodes", "172.17.0.5").set("es.port", "9200").set("es.http.timeout",
// "5m")
// .set("es.scroll.size", "50");
// JavaSparkContext sc = new JavaSparkContext(sparkConf);
//
// Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
// Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San
// Fran");
//
// JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(numbers,
// airports));
// JavaEsSpark.saveToEs(javaRDD, "spark/docs");
//
// }
//
// }
