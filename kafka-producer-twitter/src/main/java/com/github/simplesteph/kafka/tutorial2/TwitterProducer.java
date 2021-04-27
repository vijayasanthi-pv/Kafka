package com.github.simplesteph.kafka.tutorial2;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	String consumerKey = "HqJ2EcWduuOnrR5DSjhpMwDZl";
	String consumerSecret = "6DtLQUDkxHBRAISMxJl5PBdQS7KKzOHnpPmUEJPb2eEkBHVIlk";
	String token = "1278643524213395458-cGySOvf1iYeQhd3SiYEMB8g6LZfrlh";
	String tokenSecret = "dTEFS7O61DXQ879yo8Nfznl3FADiG8b3HkHHoZZ5bQC7B";
	
	List<String> terms = Lists.newArrayList("bitcoin","usa","politics","sport","soccer");
	
	public static void main(String[] args) {
		
		new TwitterProducer().run();
		
	}
	
	public void run() {
		
		//Setup blocking Queue; Be sure to size these properly based on expected TPS of your stream
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		
		Client hosebirdClient = createTwitterClient(msgQueue);
		
		hosebirdClient.connect();
		
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		//add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("stopping application...");
			logger.info("shutting down client from twitter ...");
			hosebirdClient.stop();
			logger.info("closing producer...");
			producer.close();
			logger.info("done!");
		}));
		
		while(!hosebirdClient.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				hosebirdClient.stop();
			}
			if(msg!=null) {
				logger.info("msg :: "+msg);
				producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg),new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if (e != null) {
							logger.error("Something bad happened ",e);
						}
					}
				});
			}
		}
		
		logger.info("End of application");
		
	}
	
	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		
		/** Declare the host you want to connect to, the endpoint, and authentication(basic auth or oauth)**/
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		
		hosebirdEndpoint.trackTerms(terms);
		
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
		
		ClientBuilder builder = new ClientBuilder()
									.name("Hosebird-Client-01")
									.hosts(hosebirdHosts)
									.authentication(hosebirdAuth)
									.endpoint(hosebirdEndpoint)
									.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		
		return hosebirdClient;
	}
	
	public KafkaProducer<String, String> createKafkaProducer(){
		
		//Create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getName());
		
		
		//Create safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise
		
		
		//High throughput producer (at the expense of a bit of latency and CPU usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size
		
		//Create the Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		return producer;
	}
}
