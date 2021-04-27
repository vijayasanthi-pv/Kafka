package com.github.simplesteph.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
	
	public static void main(String[] args) {
		
		//Create logger for the class 'ConsumerDemo'
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
		
		String groupId = "my-fourth-application";
		String topic = "first_topic";
		
		//Create Consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		//Create Kafka Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		//Consumer subscribes to the topic 'first_topic'
		consumer.subscribe(Arrays.asList(topic));
		
		//Declare Consumer records container
		ConsumerRecords<String, String> records = null;
		
		Scanner scanner = new Scanner(System.in);
		
		do {
			
				//Consumer polls the records
				records = consumer.poll(Duration.ofMillis(100));
				
				if (records != null) {
					Iterator<ConsumerRecord<String, String>> recordsIterator = records.iterator();
					
					while(recordsIterator.hasNext()) {
						logger.info("Key :: "+recordsIterator.next().key());
						logger.info("Value :: "+recordsIterator.next().value());
						logger.info("Partition :: "+recordsIterator.next().partition());
						logger.info("Offset :: "+recordsIterator.next().offset());
						logger.info("***************************************************");
					}
				}

				System.out.println("Do you want to continue(yes/no)? ");
			
			
		} while(scanner.next().equalsIgnoreCase("yes"));
		
		scanner.close();
		consumer.close();
		
	}
}
