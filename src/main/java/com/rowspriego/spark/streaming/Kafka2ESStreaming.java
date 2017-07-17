package com.rowspriego.spark.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.rowspriego.spark.model.Event;
import com.rowspriego.spark.operations.JavaSparkSessionSingleton;
import com.rowspriego.spark.operations.TransformToJSON;

import kafka.serializer.StringDecoder;

public final class Kafka2ESStreaming {

	private static Integer numArgs = 4;

	public static void main(String[] args) throws InterruptedException {

		if (args.length < numArgs) {
			System.err.println("Usage: Kafka2HdfsStreaming <broker> <elasticIp> <elasticPort> <elasticKey> \n");
			System.err.println("Args length: " + args.length);
			System.exit(1);
		}

		String broker = args[0];
		String elasticIp = args[1];
		String elasticPort = args[2];
		String elasticKey = args[3];

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		//////// 1. CONFIGURACION DEL CONTEXTO DE SPARK STREAMING + KAFKA //////

		SparkConf sparkConf = new SparkConf().setAppName("Kafka2ESStreaming");
		// INFO: solo para pruebas locales quitar al hacer el jar!
		sparkConf.setMaster("local[2]");
		sparkConf.set("hadoop.home.dir", "/home/bigdata/hadoop-2.7.3");
		sparkConf.set("es.index.auto.create", "true");
		sparkConf.set("es.nodes", elasticIp);
		sparkConf.set("es.port", elasticPort);
		sparkConf.set("es.http.timeout", "5m");
		sparkConf.set("es.scroll.size", "50");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

		jssc.checkpoint("/tmp/" + jssc.sparkContext().appName());

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", broker);

		String passportEventTopic = "PassportEvents";
		Set<String> passportEventsTopicSet = new HashSet<>(Arrays.asList(passportEventTopic.split(",")));
		JavaPairInputDStream<String, String> passportEventsMessages = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, passportEventsTopicSet);

		JavaDStream<Event> passportEventsLite = passportEventsMessages.map(new TransformToJSON());

		passportEventsLite.foreachRDD(new VoidFunction2<JavaRDD<Event>, Time>() {
			@Override
			public void call(JavaRDD<Event> rdd, Time time) throws Exception {
				if (!rdd.isEmpty()) {
					SparkSession spark = new JavaSparkSessionSingleton().getInstance(rdd.context().getConf());
					Dataset<Row> event = spark.createDataFrame(rdd, Event.class);
					JavaEsSpark.saveToEs(rdd, elasticKey);
				}
			}
		});

		jssc.start();
		jssc.awaitTermination();

	}

}
