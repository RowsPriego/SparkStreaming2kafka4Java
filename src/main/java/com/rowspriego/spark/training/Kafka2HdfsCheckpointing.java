package com.rowspriego.spark.training;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.zookeeper.data.Stat;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.Tuple2;

public class Kafka2HdfsCheckpointing {

	private static Integer numArgs = 4;
	private static int ZK_TIMEOUT_MSEC = 10000;
	private static String zkHosts = "172.17.0.2:2181";
	private static String zkPath = "/offsets";
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
	private static ZkClient zkClient = new ZkClient(zkHosts, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC);

	public static <K, M> void main(String[] args) throws InterruptedException {

		if (args.length < numArgs) {
			System.err.println("Usage: Kafka2HdfsStreaming <broker> <hdfs path> <window duration> <slide duration> \n");
			System.err.println("Args length: " + args.length);
			System.exit(1);
		}

		String broker = args[0];
		String hdfsPath = args[1];
		Long windowDuration = Long.valueOf(args[2]);
		Long slideDuration = Long.valueOf(args[3]);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");

		System.out.println("broker: " + broker);
		System.out.println("hdfsPath: " + hdfsPath);
		System.out.println("windowDuration: " + windowDuration);
		System.out.println("slideDuration: " + slideDuration);

		Logger.getLogger("org")
				.setLevel(Level.OFF);
		Logger.getLogger("akka")
				.setLevel(Level.OFF);

		SparkConf sparkConf = new SparkConf().setAppName("Kafka2HdfsStreaming");
		// INFO: solo para pruebas locales quitar al hacer el jar!
		sparkConf.setMaster("local[2]");

		// ******* Configuración optima para spark + kafka ********* ///
		// This enables the Spark Streaming to control the receiving rate based
		// on the current batch scheduling delays and processing times so that
		// the system receives only as fast as the system can process.
		// Internally, this dynamically sets the maximum receiving rate of
		// receivers. This rate is upper bounded by the values
		// spark.streaming.receiver.maxRate and
		// spark.streaming.kafka.maxRatePerPartition if they are set
		sparkConf.set("spark.streaming.backpressure.enabled", "true");
		// This is the initial maximum receiving rate at which each receiver
		// will receive data for the first batch when the backpressure mechanism
		// is enabled.
		// FIXME: por parámetro?
		sparkConf.set("spark.streaming.backpressure.initialRate", "10000");

		// Batch interval = 1 sec
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		jsc.checkpoint("/home/bigdata/tmp/" + jsc.sparkContext()
				.appName());

		Map<String, String> kafkaParams = new HashMap<>();
		// Lista de kafka brokers
		String brokerList = "172.17.0.3:9092,172.17.0.4:9092,172.17.0.5:9092";
		kafkaParams.put("metadata.broker.list", brokerList);
		/// kafkaParams.put("key.deserializer",
		/// "org.apache.kafka.common.serialization.StringDeserializer.class");
		/// kafkaParams.put("value.deserializer",
		/// "org.apache.kafka.common.serialization.StringDeserializer.class");
		/// kafkaParams.put("enable.auto.commit", "false");
		/// kafkaParams.put("auto.offsets.reset", "latest");
		// Como encaja aquí el tema de los group.id ???

		// DStream por cada topic
		String topic = "rowsTest";
		// Crear el topic: ./bin/kafka-topics.sh --create --zookeeper
		// 172.17.0.2:2181 --replication-factor 2 --partitions 3 --topic
		// rowsTest

		JavaDStream<String> messages = null;

		Set<String> topicSet = new HashSet<>(Arrays.asList(topic.split(",")));

		Map<TopicAndPartition, Long> myOffsets = readOffsets(topic);
		if (myOffsets != null) {

			messages = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class,
					StringDecoder.class, String.class, kafkaParams, myOffsets,
					new Function<MessageAndMetadata<String, String>, String>() {
						@Override
						public String call(MessageAndMetadata<String, String> msgAndMd) {
							return msgAndMd.message();
						}
					});

		} else {

			messages = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class,
					StringDecoder.class, kafkaParams, topicSet)
					.map(r -> r._2);

		}

		messages.foreachRDD(rdd -> saveOffsets(rdd));
		// messages.print();

		messages.reduceByWindow((a, b) -> a + "/n" + b, Durations.seconds(30), Durations.seconds(30))
				.foreachRDD((rdd, Time) -> saveInHDSF(rdd, Time, topic, hdfsPath));

		//////// START COMPUTATION ////////
		jsc.start();
		jsc.awaitTermination();
	}

	private static void saveInHDSF(JavaRDD<String> rdd, Time time, String topicName, String hdfsPath) {
		if (!rdd.isEmpty()) {
			Date resultdate = new Date(time.milliseconds());
			rdd.saveAsTextFile(hdfsPath + "/" + topicName + "/" + "/" + sdf.format(resultdate) + "/" + time.toString()
					.split(" ")[0]);
		}
	}

	private static Map<TopicAndPartition, Long> readOffsets(String topic) {

		Tuple2<Option<String>, Stat> o = ZkUtils.readDataMaybeNull(zkClient, zkPath + "_" + topic);
		Map<TopicAndPartition, Long> allOffsets = new HashMap<TopicAndPartition, Long>();

		// Feo feo pero funciona: por si no existiera el directorio, no devuelve
		// Option, si no None, cosas de scala....
		if (o._1()
				.getClass()
				.getName()
				.equals("scala.None$")) {

			return null;

		} else {

			String valores = o._1()
					.get();
			String[] valoresArray = valores.split(",");
			for (int i = 0; i < valoresArray.length; i++) {
				String valorN = valoresArray[i];
				String[] tp = valorN.split(":");
				allOffsets.put(new TopicAndPartition(topic, Integer.valueOf(tp[0])), Long.valueOf(tp[1]));
			}

			// Arrays.stream(o._1()
			// .get()
			// .split(","))
			// .map(s -> s.split(":", 2))
			// .map(array -> allOffsets.put(new TopicAndPartition(topic,
			// Integer.valueOf(array[0])),
			// Long.valueOf(array[1])));

			return allOffsets;
		}

	}

	private static Object write(String[] split) {
		System.out.println(split);
		return null;
	}

	private static void saveOffsets(JavaRDD<String> javaRDD) {
		OffsetRange[] offsets = ((HasOffsetRanges) javaRDD.rdd()).offsetRanges();
		String offsetsString = Arrays.stream(offsets)
				.map(s -> s.partition() + ":" + s.fromOffset())
				.collect(Collectors.joining(","));

		ZkUtils.updatePersistentPath(zkClient, zkPath + "_" + offsets[0].topic(), offsetsString);
	}

}
