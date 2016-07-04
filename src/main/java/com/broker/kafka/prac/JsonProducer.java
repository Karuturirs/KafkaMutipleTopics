package com.broker.kafka.prac;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.broker.kafka.prac.utils.PropertyFileReader;

	public class JsonProducer {
		 static Logger defaultLogger =
		 LoggerFactory.getLogger(JsonProducer.class);
		 static Logger kafkaLogger =
		 LoggerFactory.getLogger("com.example.kafkaLogger");
		
	 	private final ConsumerConnector consumer;
	 	private final String topic;

	public JsonProducer(String zookeeper, String topic) throws FileNotFoundException, IOException {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, "gct_GroupId"));
		this.topic = topic;
		
	}

	private static ConsumerConfig createConsumerConfig(String zookeeper,String groupId) throws FileNotFoundException, IOException {
		PropertyFileReader propload =new PropertyFileReader();
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", propload.getProperty("session.timeout.ms"));
		props.put("zookeeper.sync.time.ms",propload.getProperty("sync.time.ms"));
		props.put("auto.commit.interval.ms", propload.getProperty("auto.commit.interval.ms"));

		return new ConsumerConfig(props);

	}

	public boolean constructAndSendMessage(String topic1, String object)
		throws InterruptedException, ExecutionException, IOException {
		 try{
			 PropertyFileReader propload =new PropertyFileReader();
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, propload.getProperty("Host")+":"+propload.getProperty("BoostrapPort"));
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		boolean sync = false;
		String topic = topic1;
		String value = object;
			
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, value);
	
		if (sync) {
			producer.send(producerRecord).get();
	
		} else {
			producer.send(producerRecord);
	
		}
			producer.close();
		  	return true;
		  	
		}	
		  catch(Exception e) {
		  e.printStackTrace();
		  return false;
		  }
			
		}
	}
