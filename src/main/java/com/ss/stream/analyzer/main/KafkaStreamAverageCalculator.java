package com.ss.stream.analyzer.main;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ss.stream.analyzer.kafka.KafkaProducer;
import com.ss.stream.analyzer.model.SparkStreamingStatisticsProcessingResult;

import scala.Tuple2;

public class KafkaStreamAverageCalculator {
	static KafkaProducer kafkaProducer;
	static ObjectMapper mapper;

	public static void main(String[] args) throws Exception {

		if (args.length < 4) {
			args = new String[4];
			// TODO - for local tests on windows
			/*
			System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
			args[0] = "localhost:2181";
			args[1] = "testKafkaGroupName";
			args[2] = "bms";
			args[3] = "4";
			*/
			args[0] = "hdp-16.tap-psnc.net:2181";
			args[1] = "testKafkaGroupName";
			args[2] = "bms";
			args[3] = "4";
		}

		int numThreads = Integer.parseInt(args[3]);
		Map<String, Integer> topicMap = new HashMap<>();
		String[] topics = args[2].split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}

		SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount").setMaster("local[*]").setSparkHome("/usr/hdp/current/spark2-client/");

		// Create the context with 2 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1],
				topicMap);

		// Required for 'updateByKey'
		jssc.checkpoint("./spark_cache");

		// ----------------------------------------------------------------------------------------------------------------

		/*
		 * Initial method for processing - based on 'messages'
		 * JavaPairReceiverInputDStream collection we wre creating Tuple2
		 * objects with Key(String) and Value(Double) received by using JSON
		 * transformation with getReadTagIdFromStreamRecord and
		 * getValueFromStreamRecord methods
		 */
		JavaPairDStream<String, Double> signal = messages.mapToPair(
				s -> new Tuple2<String, Double>(getReadTagIdFromStreamRecord(s._2), getValueFromStreamRecord(s._2)));

		/*
		 * We maintain a "stats" variable with many statistical variables.
		 * Includes calculating a running mean, variance, sum, count, min, max
		 * and squared sum. updateStateKey takes the current "state" from the
		 * object we *assign* to (in this case "stats"), and the values (as a
		 * list) from the calling object (in this case "signal"). It then goes
		 * through each key and applies the operations specified within the
		 * curly braces.
		 *
		 * Some hints on how to use updateStateByKey from here:
		 * http://vidaha.gitbooks.io/spark-logs-analyzer/chapter1/java8/src/main
		 * /java/com/databricks/apps/logs/chapter1/LogAnalyzerStreamingTotal.
		 * java
		 *
		 * There is a possibility we might overflow at some point if the stream
		 * is long enough (squared sum in particular grows quite fast), but this
		 * is not taken into consideration here.
		 */

		JavaPairDStream<String, Double[]> stats = signal.updateStateByKey((values, state) -> {
			// We try to grab the previous state (for this mac address), and if
			// we can't get it (=null) we initialize it.
			Double[] prevstate = state.or(new Double[] { 0.0, 0.0, 0.0, -10.0, -150.0, 0.0, 0.0, 0.0 });
			// Sum
			double summer = prevstate[0];
			// Count
			double counter = prevstate[1];
			// Sum of squares (legacy variable, used for calculating variance in
			// batch)
			double sumsqr = prevstate[2];
			// Best minimum (smallest value) encountered
			double bestmin = prevstate[3];
			// Largest value
			double bestmax = prevstate[4];
			// Average
			double mean = prevstate[5];
			// Helper variable, for variance calculation (initialization is
			// useless, but the interpreter would do this anyway)
			double delta = 0.0;
			// Helper variable
			double m2 = prevstate[6];
			// Variance
			double var = prevstate[7];
			/*
			 * Online variance calculation from:
			 * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
			 * "This algorithm is due to Knuth"
			 */

			// Given the list of new values (which we just read and put into the
			// "signal" batch), update the stats
			for (Double s : values) {
				summer += s;
				counter += 1;
				sumsqr += s * s;
				delta = s - mean;
				mean = mean + delta / counter;
				m2 = m2 + delta * (s - mean);
				bestmin = Math.min(bestmin, s);
				bestmax = Math.max(bestmax, s);
			}

			// Avoid a divide-by-zero error in variance calculation
			if (counter > 1) {
				var = m2 / (counter - 1);
			} else {
				var = 0.0;
			}
			// We return the new stats array for this mac and move on
			// return Optional.of(new
			// Double[]{summer,counter,sumsqr,bestmin,bestmax,mean,m2,var});

			// TODO - produce Kafka message before returning optional
			// Preparing kafka producer objects

			if (kafkaProducer == null) {
				kafkaProducer = new KafkaProducer();
				kafkaProducer.initializeProducer("localhost:9092", "localhost:2181");
			}
			if (mapper == null) {
				mapper = new ObjectMapper();
			}
			
			SparkStreamingStatisticsProcessingResult sssProcessingResult = new SparkStreamingStatisticsProcessingResult(
					"test", summer, counter, sumsqr, delta, bestmin, bestmax, mean, m2, var);
			String sssProcesingResultJSON = getJSONObjectFromCurrentStatisticsData(mapper, sssProcessingResult);
			kafkaProducer.produceMessage("stream_processing_results", "key", sssProcesingResultJSON);

			return Optional.of(new Double[] { summer, counter, sumsqr, bestmin, bestmax, mean, m2, var });
		});

		/*
		 * An alternative, "naive", variance calculation, where we just
		 * calculate the variance from the calculated stats. This *could* have
		 * some use, since an online estimator is only approximate.
		 * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance The
		 * important part: variance = (Sum_sqr - (Sum*Sum)/n)/(n - 1) which for
		 * us translates to: s._2[0] = Sum, s._2[1] = n, s._2[2] = Sum_sqr
		 */
		// JavaPairDStream<String, Double> naive_vars = stats.mapToPair(s -> new
		// Tuple2<String,Double>(s._1,(s._2[2]-(s._2[0]*s._2[0])/s._2[1])/(s._2[1]-1)));

		// We create a convenience variable for neater output when writing
		JavaPairDStream<String, List<Double>> stat_writer = stats
				.mapToPair(s -> new Tuple2<String, List<Double>>(s._1, Arrays.asList(s._2)));

		stat_writer.print();
		jssc.start();
		jssc.awaitTermination();
	}

	private static String getJSONObjectFromCurrentStatisticsData(ObjectMapper mapper,
			SparkStreamingStatisticsProcessingResult sssProcessingResult) throws JsonProcessingException {

		String jsonInString = mapper.writeValueAsString(sssProcessingResult);

		return jsonInString;
	}

	private static String getReadTagIdFromStreamRecord(Object s) {
		JSONObject jsonObject = new JSONObject(String.valueOf(s));
		String readTagId = jsonObject.getString("readTag_id");
		return readTagId;
	}

	private static Double getValueFromStreamRecord(Object s) {
		JSONObject jsonObject = new JSONObject(String.valueOf(s));
		String value = jsonObject.getString("readValue");
		Double doubleValue = Double.parseDouble(value);

		// transform to double
		return doubleValue;
	}

}
