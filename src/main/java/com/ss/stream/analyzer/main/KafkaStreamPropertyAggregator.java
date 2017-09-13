package com.ss.stream.analyzer.main;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.JSONObject;

import scala.Tuple2;

public class KafkaStreamPropertyAggregator {
	private static final Pattern SPACE = Pattern.compile(" ");

	private KafkaStreamPropertyAggregator() {
	}

	public static void main(String[] args) throws Exception {

		args = new String[4];
		args[0] = "localhost:2181";
		args[1] = "testKafkaGroupName";
		args[2] = "bms";
		args[3] = "4";

		if (args.length < 4) {
			System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
			System.exit(1);
		}

		int numThreads = Integer.parseInt(args[3]);
		Map<String, Integer> topicMap = new HashMap<>();
		String[] topics = args[2].split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}

		SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount").setMaster("local[2]");

		// Create the context with 2 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1],
				topicMap);

		// ----------------------------------------------------------------------------------------------------------------

		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> tuple2) {
				JSONObject jsonObj = new JSONObject(tuple2._2);
				String readTag_id = jsonObj.getString("readTag_id");

				// return tuple2._2();
				return readTag_id;
			}
		});

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String x) {
				Iterator<String> iteratorOfStrings = Arrays.asList(SPACE.split(x)).iterator();
				return iteratorOfStrings;
			}
		});

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String s) {
				Tuple2<String, Integer> tuple = new Tuple2<>(s, 1);
				return tuple;
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		wordCounts.print();

		jssc.start();
		jssc.awaitTermination();
	}
}
