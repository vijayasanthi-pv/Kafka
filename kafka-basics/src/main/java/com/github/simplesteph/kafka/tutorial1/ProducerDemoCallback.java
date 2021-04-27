package com.github.simplesteph.kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoCallback {

	public static void main(String[] args) {
		
		//Create logger for the class 'ProducerDemoCallback'
		final Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);
		
		//Create the producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Create the kafka producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		//Create the producer record
		ProducerRecord<String, String> record;
		
		for (int count=0; count < 10; count++) {
		
			record = new ProducerRecord<String, String>("first_topic", "hello world "+count);
			
			//send data - asynchronous with a call back
			producer.send(record, new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						logger.info("Received new metadata. \n" +
									"Topic:" + metadata.topic() + "\n" +
									"Partition:" + metadata.partition() + "\n" +
									"Offset:" + metadata.offset() + "\n" +
									"Timestamp:" + metadata.timestamp());
					}else {
						logger.error(exception.toString());
					}
				}
			});
		}
		
		//flush data
		producer.flush();
		
		//flush and close producer
		producer.close();
		
	}
}
