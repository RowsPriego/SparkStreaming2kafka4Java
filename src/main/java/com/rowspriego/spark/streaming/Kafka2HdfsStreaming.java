package com.rowspriego.spark.streaming;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
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

import com.rowspriego.spark.model.Event;
import com.rowspriego.spark.operations.ConcatenateTopics;
import com.rowspriego.spark.operations.GetJson;
import com.rowspriego.spark.operations.JavaSparkSessionSingleton;
import com.rowspriego.spark.operations.TransformToJSON;

import kafka.serializer.StringDecoder;

public class Kafka2HdfsStreaming {

	private static Integer numArgs = 4;

	public static void main(String[] args) throws InterruptedException {

		if (args.length < numArgs) {
			System.err.println("Usage: Kafka2HdfsStreaming <broker> <hdfs path> <window duration> <slide duration> \n");
			System.err.println("Args length: " + args.length);
			System.exit(1);
		}

		String broker = args[0];
		String hdfsPath = args[1];
		Long windowDuration = Long.valueOf(args[2]);
		Long slideDuration = Long.valueOf(args[3]);

		System.out.println("broker: " + broker);
		System.out.println("hdfsPath: " + hdfsPath);
		System.out.println("windowDuration: " + windowDuration);
		System.out.println("slideDuration: " + slideDuration);

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");

		SparkConf sparkConf = new SparkConf().setAppName("Kafka2HdfsStreaming");

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
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		jssc.checkpoint("hdfs://172.17.0.9:9000/spark/checkpoint/" + jssc.sparkContext().appName());

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", broker);

		// DStream por cada topic
		String rawEventTopic = "RawEvents";
		Set<String> rawEventsTopicSet = new HashSet<>(Arrays.asList(rawEventTopic.split(",")));
		JavaPairInputDStream<String, String> rawEventsMessages = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, rawEventsTopicSet);
		JavaDStream<String> rawEventstoSave = rawEventsMessages.map(new GetJson());

		// Save in HDFS plain text with non-overlapping windows (set by
		// parameter)
		rawEventstoSave.reduceByWindow(new ConcatenateTopics(), Durations.seconds(windowDuration),
				Durations.seconds(slideDuration)).foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {

					@Override
					public void call(JavaRDD<String> rdd, Time time) throws Exception {
						if (!rdd.isEmpty()) {

							Date resultdate = new Date(time.milliseconds());
							rdd.saveAsTextFile(hdfsPath + "/" + rawEventTopic + "/" + "/" + sdf.format(resultdate) + "/"
									+ time.toString().split(" ")[0]);
						}

					}
				});

		String passportEventTopic = "PassportEvents";
		Set<String> passportEventsTopicSet = new HashSet<>(Arrays.asList(passportEventTopic.split(",")));
		JavaPairInputDStream<String, String> passportEventsMessages = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, passportEventsTopicSet);
		JavaDStream<String> passportEventstoSave = passportEventsMessages.map(new GetJson());

		// Save in HDFS plain text with non-overlapping windows (set per
		// parameter)
		passportEventstoSave.reduceByWindow(new ConcatenateTopics(), Durations.seconds(windowDuration),
				Durations.seconds(slideDuration)).foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {

					@Override
					public void call(JavaRDD<String> rdd, Time time) throws Exception {
						if (!rdd.isEmpty()) {
							Date resultdate = new Date(time.milliseconds());
							rdd.saveAsTextFile(hdfsPath + "/" + passportEventTopic + "/" + "/" + sdf.format(resultdate)
									+ "/" + time.toString().split(" ")[0]);
						}
					}
				});

		String sessionsResTopic = "SessionsRes";
		Set<String> sessionResTopicSet = new HashSet<>(Arrays.asList(sessionsResTopic.split(",")));
		JavaPairInputDStream<String, String> sessionsResMessages = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, sessionResTopicSet);
		JavaDStream<String> sessionsRestoSave = sessionsResMessages.map(new GetJson());

		// Save in HDFS plain text with non-overlapping windows (set per
		// parameter)
		sessionsRestoSave.reduceByWindow(new ConcatenateTopics(), Durations.seconds(windowDuration),
				Durations.seconds(slideDuration)).foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {

					@Override
					public void call(JavaRDD<String> rdd, Time time) throws Exception {
						if (!rdd.isEmpty()) {
							Date resultdate = new Date(time.milliseconds());
							rdd.saveAsTextFile(hdfsPath + "/" + sessionsResTopic + "/" + "/" + sdf.format(resultdate)
									+ "/" + time.toString().split(" ")[0]);
						}
					}
				});

		String getRecommendationTopic = "RecommendationReq";
		Set<String> getRecommendationTopicSet = new HashSet<>(Arrays.asList(getRecommendationTopic.split(",")));
		JavaPairInputDStream<String, String> getRecommendationMessages = KafkaUtils.createDirectStream(jssc,
				String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams,
				getRecommendationTopicSet);
		JavaDStream<String> recommendationReqtoSave = getRecommendationMessages.map(new GetJson());

		// Save in HDFS plain text with non-overlapping windows (set per
		// parameter)
		recommendationReqtoSave.reduceByWindow(new ConcatenateTopics(), Durations.seconds(windowDuration),
				Durations.seconds(slideDuration)).foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {

					@Override
					public void call(JavaRDD<String> rdd, Time time) throws Exception {
						if (!rdd.isEmpty()) {
							Date resultdate = new Date(time.milliseconds());
							rdd.saveAsTextFile(hdfsPath + "/" + getRecommendationTopic + "/" + "/"
									+ sdf.format(resultdate) + "/" + time.toString().split(" ")[0]);
						}
					}
				});

		String recommendationResTopic = "RecommendationRes";
		Set<String> recommendationResTopicSet = new HashSet<>(Arrays.asList(recommendationResTopic.split(",")));
		JavaPairInputDStream<String, String> recommendationResMessages = KafkaUtils.createDirectStream(jssc,
				String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams,
				recommendationResTopicSet);
		JavaDStream<String> recommendationRestoSave = recommendationResMessages.map(new GetJson());

		// Save in HDFS plain text with non-overlapping windows (set per
		// parameter)
		recommendationRestoSave.reduceByWindow(new ConcatenateTopics(), Durations.seconds(windowDuration),
				Durations.seconds(slideDuration)).foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {

					@Override
					public void call(JavaRDD<String> rdd, Time time) throws Exception {
						if (!rdd.isEmpty()) {
							Date resultdate = new Date(time.milliseconds());
							rdd.saveAsTextFile(hdfsPath + "/" + recommendationResTopic + "/" + "/"
									+ sdf.format(resultdate) + "/" + time.toString().split(" ")[0]);
						}
					}
				});

		String contextualizedEventTopic = "ContextualizedEvents";
		Set<String> contextualizedEventTopicSet = new HashSet<>(Arrays.asList(contextualizedEventTopic.split(",")));
		JavaPairInputDStream<String, String> contextualizedEventMessages = KafkaUtils.createDirectStream(jssc,
				String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams,
				contextualizedEventTopicSet);
		JavaDStream<String> contextualizedEventstoSave = contextualizedEventMessages.map(new GetJson());

		// Save in HDFS plain text with non-overlapping windows (set per
		// parameter)
		contextualizedEventstoSave.reduceByWindow(new ConcatenateTopics(), Durations.seconds(windowDuration),
				Durations.seconds(slideDuration)).foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {

					@Override
					public void call(JavaRDD<String> rdd, Time time) throws Exception {
						if (!rdd.isEmpty()) {
							Date resultdate = new Date(time.milliseconds());
							rdd.saveAsTextFile(hdfsPath + "/" + contextualizedEventTopic + "/" + "/"
									+ sdf.format(resultdate) + "/" + time.toString().split(" ")[0]);
						}
					}
				});

		String profiledEventTopic = "ProfiledEvents";
		Set<String> profiledEventTopicSet = new HashSet<>(Arrays.asList(profiledEventTopic.split(",")));
		JavaPairInputDStream<String, String> profiledEventMessages = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, profiledEventTopicSet);
		JavaDStream<String> profiledEventstoSave = profiledEventMessages.map(new GetJson());

		// Save in HDFS plain text with non-overlapping windows (set per
		// parameter)
		profiledEventstoSave.reduceByWindow(new ConcatenateTopics(), Durations.seconds(windowDuration),
				Durations.seconds(slideDuration)).foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {

					@Override
					public void call(JavaRDD<String> rdd, Time time) throws Exception {
						if (!rdd.isEmpty()) {
							Date resultdate = new Date(time.milliseconds());
							rdd.saveAsTextFile(hdfsPath + "/" + profiledEventTopic + "/" + "/" + sdf.format(resultdate)
									+ "/" + time.toString().split(" ")[0]);
						}
					}
				});

		String getRecommendationRnowTopic = "RecomendationsRnow";
		Set<String> getRecommendationRnowTopicSet = new HashSet<>(Arrays.asList(getRecommendationRnowTopic.split(",")));
		JavaPairInputDStream<String, String> getRecommendationRnowMessages = KafkaUtils.createDirectStream(jssc,
				String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams,
				getRecommendationRnowTopicSet);
		JavaDStream<String> recommendationRnowtoSave = getRecommendationRnowMessages.map(new GetJson());

		// Save in HDFS plain text with non-overlapping windows (set per
		// parameter)
		recommendationRnowtoSave.reduceByWindow(new ConcatenateTopics(), Durations.seconds(windowDuration),
				Durations.seconds(slideDuration)).foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {

					@Override
					public void call(JavaRDD<String> rdd, Time time) throws Exception {
						if (!rdd.isEmpty()) {
							Date resultdate = new Date(time.milliseconds());
							rdd.saveAsTextFile(hdfsPath + "/" + getRecommendationRnowTopic + "/" + "/"
									+ sdf.format(resultdate) + "/" + time.toString().split(" ")[0]);
						}
					}
				});

		String recommendationSendTopic = "RecomendationsSend";
		Set<String> recommendationSendSet = new HashSet<>(Arrays.asList(recommendationSendTopic.split(",")));
		JavaPairInputDStream<String, String> recommendationSendMessages = KafkaUtils.createDirectStream(jssc,
				String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams,
				recommendationSendSet);
		JavaDStream<String> recommendationSendtoSave = recommendationSendMessages.map(new GetJson());

		// Save in HDFS plain text with non-overlapping windows (set per
		// parameter)
		recommendationSendtoSave.reduceByWindow(new ConcatenateTopics(), Durations.seconds(windowDuration),
				Durations.seconds(slideDuration)).foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {

					@Override
					public void call(JavaRDD<String> rdd, Time time) throws Exception {
						if (!rdd.isEmpty()) {
							Date resultdate = new Date(time.milliseconds());
							rdd.saveAsTextFile(hdfsPath + "/" + recommendationSendTopic + "/" + "/"
									+ sdf.format(resultdate) + "/" + time.toString().split(" ")[0]);
						}
					}
				});

		// PARQUET

		JavaDStream<Event> passportEventsLite = passportEventsMessages.map(new TransformToJSON());

		passportEventsLite.foreachRDD(new VoidFunction2<JavaRDD<Event>, Time>() {

			@Override
			public void call(JavaRDD<Event> rdd, Time time) throws Exception {

				if (!rdd.isEmpty()) {
					SparkSession spark = new JavaSparkSessionSingleton().getInstance(rdd.context().getConf());

					Date resultdate = new Date(time.milliseconds());
					Dataset<Row> event = spark.createDataFrame(rdd, Event.class);
					event.createOrReplaceTempView("events");

					event.toDF().write().parquet(hdfsPath + "/" + rawEventTopic + "/parquet/" + sdf.format(resultdate)
							+ "/" + time.toString().split(" ")[0]);
				}

			}
		});

		jssc.start();
		jssc.awaitTermination();
	}

}
