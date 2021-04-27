package com.github.simplesteph.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerInConsumerGroup {

	public static void main(String[] args) {
		
		//Create Logger for 'ConsumerInConsumerGroup' class
		Logger logger = LoggerFactory.getLogger(ConsumerInConsumerGroup.class);
		
		String groupId = "my-fifth-application";
		String topic = "first_topic";
		
		//Create properties for Consumer
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		//Create Kafka Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		consumer.subscribe(Arrays.asList(topic));
		
		ConsumerRecords<String, String> records = null;
		Iterator<ConsumerRecord<String, String>> recordsIterator = null;
		ConsumerRecord<String, String> record = null;
		
		//This code will leave a memory leak as the consumer cannot be closed in the loop
		while(true) {
			
			//Poll the consumer
			records = consumer.poll(Duration.ofMillis(100));
			
			recordsIterator = records.iterator();
			
			while (recordsIterator.hasNext()) {
				
				record = recordsIterator.next();
				
				logger.info("Key :: "+record.key());
				logger.info("Value :: "+record.value());
				logger.info("Partition :: "+record.partition());
				logger.info("Offset :: "+record.offset());
				logger.info("***************************************************");
			}
		}
		
	}
}
