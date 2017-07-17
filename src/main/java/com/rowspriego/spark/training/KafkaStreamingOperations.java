package com.rowspriego.spark.training;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.rowspriego.spark.model.Event;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaStreamingOperations {

	public static void main(String[] args) throws InterruptedException {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		//////// 1. CONFIGURACION DEL CONTEXTO DE SPARK STREAMING + KAFKA //////

		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("eturKafkaStreaming");
		// https://spark.apache.org/docs/latest/tuning.html#data-serialization
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
		// Configuración de checkpoint.
		/*
		 * INFO: Es necesaria en aplicaciones spark streaming para asegurar la
		 * tolerancia a fallos
		 * http://spark.apache.org/docs/latest/streaming-programming-guide.html#
		 * checkpointing Sobre esto habría que documentarse más
		 */
		jssc.checkpoint("/home/bigdata/checkpoint");

		String brokers = "172.17.0.3:9092";
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);

		//////// 2. CREACION DEL STREAM (UNO POR CADA TOPIC) ////////

		/*
		 * INFO: se puede crear una variable String topics =
		 * "sessionsResTopic,PassportEvents,SessionsRes" y en la creación del
		 * directStream leer de todos los topics a la vez, pero luego no es
		 * evidente distinguir de que topic en concreto está leyendo (hace falta
		 * una configuración extra en la creación del DirectStream), así que se
		 * crea un directStream por cada topic Segun la documentación, la
		 * creación de distintos streams hace que se lean en paralelo: "Note
		 * that, if you want to receive multiple streams of data in parallel in
		 * your streaming application, you can create multiple input DStreams"
		 * ->
		 * http://spark.apache.org/docs/latest/streaming-programming-guide.html#
		 * input-dstreams-and-receivers "For example, a single Kafka input
		 * DStream receiving two topics of data can be split into two Kafka
		 * input streams, each receiving only one topic"
		 * http://spark.apache.org/docs/latest/streaming-programming-guide.html#
		 * level-of-parallelism-in-data-receiving
		 */
		String rawEventTopic = "RawEvents";
		Set<String> rawEventsTopicSet = new HashSet<>(Arrays.asList(rawEventTopic.split(",")));
		JavaPairInputDStream<String, String> rawEventsMessages = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, rawEventsTopicSet);

		String passportEventTopic = "PassportEvents";
		Set<String> passportEventsTopicSet = new HashSet<>(Arrays.asList(passportEventTopic.split(",")));
		JavaPairInputDStream<String, String> passportEventsMessages = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, passportEventsTopicSet);

		String sessionsResTopic = "SessionsRes";
		Set<String> sessionResTopicSet = new HashSet<>(Arrays.asList(sessionsResTopic.split(",")));
		JavaPairInputDStream<String, String> sessionsResMessages = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, sessionResTopicSet);

		// DEBUG: rawEventsMessages.print();
		// DEBUG: passportEventsTopicSet.print();
		// DEBUG: sessionResTopicSet.print();

		//////// 3. TRANSFORMACIONES ////////
		/*
		 * Internally, a DStream is represented by a continuous series of RDDs
		 * http://spark.apache.org/docs/latest/streaming-programming-guide.html#
		 * discretized-streams-dstreams *
		 * 
		 * Similar to that of RDDs, transformations allow the data from the
		 * input DStream to be modified. DStreams support many of the
		 * transformations available on normal Spark RDD’s. Some of the common
		 * ones are as follows [...]
		 * http://spark.apache.org/docs/latest/streaming-programming-guide.html#
		 * transformations-on-dstreams
		 */

		// Transformación de tipo MAP: dado el json entero, lo transformo a un
		// json más simple que tiene lo que necesito para las estadísticas

		// Se crea la función de tranformación del JSON a JSON resumido como una
		// clase para no tener que definirla para cada topic

		// TODO: implementar clases que hereden de esta para redefinir
		// comportamiento por cada tipo de topic

		class TransformToJsonLite implements Function<Tuple2<String, String>, Event> {

			@Override
			public Event call(Tuple2<String, String> arg0) throws Exception {
				Event event = new Event();
				JSONParser parser = new JSONParser();
				JSONObject json = null;

				if (arg0 != null) {
					try {
						json = (JSONObject) parser.parse(arg0._2());
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					// Modificaciones en el JSON para adaptarse a mi JSON de
					// salida

					if (json.containsKey("eid")) {
						event.setEid((String) json.get("eid"));
					}

					if (json.containsKey("passport")) {
						JSONObject passport = (JSONObject) json.get("passport");
						if (passport.containsKey("sid")) {
							event.setSessionId((String) passport.get("sid"));
						} else {
							event.setSessionId("");
						}
					} else {
						event.setSessionId("");
					}

					if (json.containsKey("userId")) {
						event.setUserId((String) json.get("userId"));
					}

					return event;
				}

				return null;
			}

		}

		// map
		JavaDStream<Event> rawEventsLite = rawEventsMessages.map(new TransformToJsonLite());
		JavaDStream<Event> passportEventsLite = passportEventsMessages.map(new TransformToJsonLite());
		JavaDStream<Event> sessionResLite = sessionsResMessages.map(new TransformToJsonLite());

		// DEBUG:
		// rawEventsLite.print();
		// passportEventsLite.print();
		// sessionResLite.print();

		// Completo la información del json de resumen con el type: que en
		// realidad es el nombre del topic al que pertenecía
		// Esto no lo podía hacer en la función genérica TransformToJsonLite,
		// porque tenía que valer para todos los topics, pero si puedo hacer una
		// transformación en cada uno de los Dstreams para informr este campo
		// pero si que puedo hacerlo por cada uno

		// TODO: hacer esto más elegante
		JavaDStream<Event> rawEventsLiteWithType = rawEventsLite.map(new Function<Event, Event>() {

			@Override
			public Event call(Event arg0) throws Exception {
				arg0.setType("rawEvent");
				return arg0;
			}

		});

		JavaDStream<Event> passportEventsLiteWithType = passportEventsLite
				.map(new Function<Event, Event>() {

					@Override
					public Event call(Event arg0) throws Exception {
						arg0.setType("passportEvent");
						return arg0;
					}

				});

		JavaDStream<Event> sessionResLiteWithType = sessionResLite.map(new Function<Event, Event>() {

			@Override
			public Event call(Event arg0) throws Exception {
				arg0.setType("sessionRes");
				return arg0;
			}

		});

		// DEBUG:
		// rawEventsLiteWithType.print();
		// passportEventsLiteWithType.print();
		// sessionResLiteWithType.print();

		// filter y countByWindow
		// Como se pueden dar diferentes operaciones, vamos a filtrar por tipo
		// de operación (eid) y contar cuantas operaciones de cada tipo hay

		JavaDStream<Long> passportEventsTotal = passportEventsLiteWithType.countByWindow(Durations.seconds(10),
				Durations.seconds(4));

		passportEventsTotal.map(new Function<Long, String>() {

			@Override
			public String call(Long count) throws Exception {
				return "Total: " + count;
			}

		});// .print();

		JavaDStream<Long> passportEventsRawEventsCount = passportEventsLiteWithType
				.filter(new Function<Event, Boolean>() {

					@Override
					public Boolean call(Event event) throws Exception {
						return (event.getEid().equals("upsert_session"));

					}

				}).countByWindow(Durations.seconds(10), Durations.seconds(4));

		passportEventsRawEventsCount.map(new Function<Long, String>() {

			@Override
			public String call(Long count) throws Exception {
				return "Eventos RawEvent: " + count;
			}

		}); // .print();

		JavaDStream<Long> passportEventsTrackEventsCount = passportEventsLiteWithType
				.filter(new Function<Event, Boolean>() {

					@Override
					public Boolean call(Event event) throws Exception {
						return (event.getEid().equals("track_event"));

					}

				}).countByWindow(Durations.seconds(10), Durations.seconds(4));

		passportEventsTrackEventsCount.map(new Function<Long, String>() {

			@Override
			public String call(Long count) throws Exception {
				return "Eventos track_device: " + count;
			}

		}); // .print();

		// Otras operaciones: repartition, union, count, reduce, countByValue,
		// reduceByKey, join, cogroup, transform, updateStateByKey, window

		// flatMap: no encontramos un caso de uso para aplicar flatmap en este
		// caso.
		// Si se leyeran X topics en cada DStream, en vez de leerlos linea a
		// linea, en vez de map se utilizaría flatMap para procesar los
		// distintos elementos de un topci

		// Cuando en los parámetros de countByWindow he puesto 10,5 segundos ha
		// dado el error:
		// The slide duration of ReducedWindowedDStream (5000 ms) must be
		// multiple of the slide duration of parent DStream (2000 ms)
		JavaDStream<Long> topicsCount = rawEventsLite.mapToPair(new PairFunction<Event, Event, Integer>() {
			@Override
			public Tuple2<Event, Integer> call(Event s) {
				return new Tuple2<>(s, 1);
			}
		}).countByWindow(Durations.seconds(10), Durations.seconds(4));

		// reduceByWindow

		// reduceByKeyAndWindow

		// countByValueAndWindow

		//////// 3. ACCIONES: OUTPUT OPERATIONS ////////

		//
		// foreachRDD saveAsHadoopFiles saveAsTextFiles

		passportEventsMessages.print();
		passportEventsMessages.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {

			@Override
			public void call(JavaPairRDD<String, String> arg0) throws Exception {

				arg0.collect();
				arg0.saveAsTextFile("hdfs://172.17.0.9:9000/spark/kafka");

			}

		});

		passportEventsLiteWithType.foreachRDD(new VoidFunction2<JavaRDD<Event>, Time>() {

			@Override
			public void call(JavaRDD<Event> arg0, Time arg1) throws Exception {
				// TODO Auto-generated method stub

			}

		});

		//////// 4. START COMPUTATION ////////
		jssc.start();
		jssc.awaitTermination();
	}

}
