package com.rowspriego.spark.streaming;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class KafkaStructuredStreaming {

	private static Integer numArgs = 1;

	public static void main(String[] args) throws StreamingQueryException {

		if (args.length < numArgs) {
			System.err.println("Usage: KafkaStructuredStreaming <KafkaBootStrapServers> <elasticIp> <elasticPort> \n");
			System.err.println("Args length: " + args.length);
			System.exit(1);
		}

		String kafkaBootstrapServerList = args[0];
		// String elasticIp = args[1];
		// String elasticPort = args[2];
		String passportEventTopic = "PassportEvents";

		Logger.getLogger("org")
				.setLevel(Level.OFF);
		Logger.getLogger("akka")
				.setLevel(Level.OFF);

		SparkSession spark = SparkSession.builder()
				.appName("KafkaStructuredStreaming")
				.master("local[2]")
				.getOrCreate();

		Dataset<String> messages = spark.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", kafkaBootstrapServerList)
				.option("subscribe", passportEventTopic)
				.load()
				.selectExpr("CAST(value AS STRING)")
				.as(Encoders.STRING());

		Dataset<Row> wordCounts = messages.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" "))
				.iterator(), Encoders.STRING())
				.groupBy("value")
				.count();

		StreamingQuery query = wordCounts.writeStream()
				.outputMode("complete")
				.format("console")
				.start();

		query.awaitTermination();
	}
}
