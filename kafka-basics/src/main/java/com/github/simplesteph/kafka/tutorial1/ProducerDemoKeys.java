package com.github.simplesteph.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		//Create logger for the class 'ProducerDemoKeys'
		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		
		//Create the producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Create Kafka Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		//Declare Producer Record
		ProducerRecord<String, String> record = null;
		
		//Declare and initialize topic, value and key
		String topic = "first_topic";
		String value = null;
		String key = null;
		
		for (int count=0; count < 10; count++) {
			
			key =  "id_" + Integer.toString(count);
			value = "hello world "+ Integer.toString(count);
			
			logger.info("Key :: "+key); //log the key
		
			//Initialise record
			record = new ProducerRecord<String, String>(topic, key, value);
			
			//asynchronous send with a callback
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
			}).get(); //block the .send() to make it synchronous - don't do this in production
		}
		
		//flush data
		producer.flush();
		
		//flush and close producer
		producer.close();
		
	}

}
