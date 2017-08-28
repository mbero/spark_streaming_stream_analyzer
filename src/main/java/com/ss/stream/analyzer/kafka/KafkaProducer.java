package com.ss.stream.analyzer.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducer {

	private Producer<String, String> producer;


	public KafkaProducer() {
	}
	/**
	 * Responsible for invoking Producer
	 * @param bootstrapServerAdress
	 * @param zookeperAdress
	 */
	public void initializeProducer(String bootstrapServerAdress, String zookeperAdress) {
		configureProducer(bootstrapServerAdress, zookeperAdress);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void configureProducer(String bootstrapServerAdress, String zookeperAdress) {
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServerAdress);
		if(zookeperAdress.equals("NONE")!=true){
			props.put("zookeper.connect", zookeperAdress);
		}
		else{
			System.out.println("zookeper.connect parameter not used during kafka producer start");
		}
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new org.apache.kafka.clients.producer.KafkaProducer(props);
	}

	/**
	 * Sends message using Kafka Producer API
	 * 
	 * @param topicName
	 * @param key
	 * @param value
	 */
	public void produceMessage(String topicName, String key, String value) {
		producer.send(new ProducerRecord<String, String>(topicName, key, value));
	}

	/**
	 * Closes
	 */
	public void closeProducer() {
		producer.close();
	}
}
