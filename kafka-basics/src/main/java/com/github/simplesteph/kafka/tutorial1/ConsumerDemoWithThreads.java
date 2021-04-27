package com.github.simplesteph.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThreads {

	//Logger to log the class 'ConsumerDemoWithThreads'
	Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
	CountDownLatch countDownLatch = null;

	ConsumerDemoWithThreads(){
		countDownLatch = new CountDownLatch(1);
	}

	public static void main(String[] args) {

		new ConsumerDemoWithThreads().run();
	}

	private void run() {

		String groupId = "my-sixth-application";
		String topic = "first_topic";
		String bootstrapServers = "localhost:9092";

		Runnable consumerWorker = new ConsumerWorker(countDownLatch, groupId, topic, bootstrapServers);
		Thread workerThread = new Thread(consumerWorker);
		workerThread.start();

		//add a Shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("Caught shutdown hook");
			((ConsumerWorker)consumerWorker).shutdown();
			try {
				countDownLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Application has exited");
		}));

		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing ");
		}
	}

	private class ConsumerWorker implements Runnable{

		CountDownLatch countDownLatch = null;
		Properties properties = null;

		KafkaConsumer<String, String> consumer = null;

		//Logger to log the class 'ConsumerWorker'
		Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);

		ConsumerWorker(CountDownLatch countDownLatch, String groupId, String topic, String bootstrapServers){

			this.countDownLatch = countDownLatch;

			properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			consumer = new KafkaConsumer<>(properties);
			consumer.subscribe(Arrays.asList(topic));

		}

		@Override
		public void run() {

			ConsumerRecords<String, String> records = null;
			Iterator<ConsumerRecord<String, String>> iterator = null;
			ConsumerRecord<String, String> record = null;

			try {
				while(true) {
					//poll for new data
					records = consumer.poll(Duration.ofMillis(100));
					iterator = records.iterator();

					while (iterator.hasNext()) {

						record = iterator.next();
						logger.info("Key :: "+record.key());
						logger.info("Value :: "+record.value());
						logger.info("Partition :: "+record.partition());
						logger.info("Offset :: "+record.offset());
						logger.info("***************************************************");

					}
				}
			}catch(WakeupException ex) {
				logger.error("Received shutdown signal !");
			}finally {
				consumer.close();
				//tell our main code we are done with the consumer
				countDownLatch.countDown();
			}
		}

		public void shutdown() {
			//the wakeup() method is a special method to interrupt consumer.poll()
			//it will throw the exception WakeUpException
			consumer.wakeup();
		}

	}

}
