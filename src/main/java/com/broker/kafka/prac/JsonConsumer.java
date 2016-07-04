package com.broker.kafka.prac;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.broker.kafka.prac.utils.PropertyFileReader;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

	public class JsonConsumer {
	private final ConsumerConnector consumer;
	private final String topic;

	public JsonConsumer(String zookeeper, String groupId, String topic) throws FileNotFoundException, IOException {
		
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper,groupId));
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

	public String testConsumer() throws IOException {
				
		String hiveinputmsg= "";
		PropertyFileReader propload =new PropertyFileReader();
				Map<String, Integer> topicMap = new HashMap<String, Integer>();

				topicMap.put(topic, new Integer(1));

				Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);

				List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);
				System.out.println("Consumer getting messages........");
				
				for (final KafkaStream<byte[], byte[]> stream : streamList) {
				ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
				while (consumerIte.hasNext()) {
	            hiveinputmsg = new String();
				String gct_msg = new String(consumerIte.next().message());
				System.out.println("New message received:: " + gct_msg);
				//msg=gct_msg;
				if(gct_msg != null)
				hiveinputmsg = buildHiveInput(gct_msg);
							     
				Format formatter = new SimpleDateFormat(propload.getProperty("DateFormat"));
				Date date = new Date();
				String s = formatter.format(date.getTime());
				String str1=s.replaceAll(" ", "-").replaceAll(":", "-");
				String str=propload.getProperty("OutPutFilePath")+"/gct_"+str1+".txt";
				BufferedWriter out = new BufferedWriter(new FileWriter(str, true));
				String test=buildHiveInput(gct_msg);
				out.write(hiveinputmsg);				
				out.newLine();
				out.flush();
				out.close();	

			}
		}
		
				if (consumer != null)
				consumer.shutdown();
				return hiveinputmsg; 

	}

	private String buildHiveInput(String gct_msg) {
		String[] GCTKeyValuePairs=gct_msg.split(",");
		   String GCTValue=null;
		   String FinalValue=null;
		   for(String KeyValuePair:GCTKeyValuePairs){
		    
		    String[] keyValuePairArray=KeyValuePair.split("=");
		    if(keyValuePairArray[0].trim().equalsIgnoreCase("fullUrl") && keyValuePairArray.length == 3){
		    GCTValue=keyValuePairArray[1]+"="+keyValuePairArray[2];
		    }else{
		    GCTValue=keyValuePairArray[1];
		    }
		    if(FinalValue!=null){
		     FinalValue+=GCTValue+"\t"; 
		    }
		    else
		     FinalValue=GCTValue+"\t"; 
		    
		   }
		   
		   System.out.println("Value for Hive: "+FinalValue);
		
		return FinalValue;
	}
}
