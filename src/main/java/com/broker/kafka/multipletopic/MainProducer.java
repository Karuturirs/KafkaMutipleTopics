package com.broker.kafka.multipletopic;

/****************************************************************************************************
 *  Initiate the zookeeper and assign the message with the topic to the broker

 *  @author: Ravi Sankar Karuturi
 *  @createddate: 12/16/2015 
 *****************************************************************************************************/
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

	public class MainProducer {
		 static Logger defaultLogger =
		 LoggerFactory.getLogger(MainProducer.class);
		 static Logger kafkaLogger =
		 LoggerFactory.getLogger("com.example.kafkaLogger");
		
	 	private final ConsumerConnector consumer;
	 	
	public MainProducer(String zookeeper) throws FileNotFoundException, IOException {
		this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, "gct_GroupId"));
		
		
	}

	private static ConsumerConfig createConsumerConfig(String zookeeper,String groupId) throws FileNotFoundException, IOException {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);

	}

	public boolean constructAndSendMessage(String topic, String object)
		throws InterruptedException, ExecutionException, IOException {
		 try{
			
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		boolean sync = false;
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
