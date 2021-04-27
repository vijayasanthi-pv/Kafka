package com.github.simplesteph.kafka.tutorial3;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {
	
	public static RestHighLevelClient createClient() {
		
		String hostname = "kafka-course-9830871769.us-east-1.bonsaisearch.net";
		String username = "d6yl8gujuu";
		String password = "is776hbmdl";
		
		//dont do if you run a local ES
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));
	
		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
											.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
												
												@Override
												public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
													httpClientBuilder.setSSLHostnameVerifier(null);
													return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
												}
											});
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
	
	public static KafkaConsumer<String, String> createConsumer(String topic){
		
		String groupId = "kafka-demo-elasticsearch";
		
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
		
		return consumer;
	}
	
	public static void main(String[] args) throws IOException {
		
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		
		RestHighLevelClient client = createClient();
		
		KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
		
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for (ConsumerRecord<String, String> record : records) {
				
				/**********Below strategies prevent record duplication in ElasticSearch*************/
				//2 strategies for Idempotence
				//kafka generic ID
				//String id = record.topic() + "_" + record.partition() + "_" + record.offset();
				
				//twitter feed specific id
				String id = extractIdFromTweet(record.value());
				
				/**********************************************************************************/
				
				//For this program to run, first run 'TwitterProducer' from 'kafka-producer-twitter module'
				//Here we insert data into Elasticsearch
				IndexRequest indexRequest = new IndexRequest(
						"twitter", "tweets", id
				).source(record.value(), XContentType.JSON);
				
				IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
				
				logger.info("id :: "+indexResponse.getId());
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		//to close the client gracefully
		//client.close();
	}

	private static JsonParser jsonParser = new JsonParser();
	
	private static String extractIdFromTweet(String tweetJson) {
		return jsonParser.parse(tweetJson)
				  .getAsJsonObject()
				  .get("id_str")
				  .getAsString();
	}
}
