package com.broker.kafka.multipletopic;

/****************************************************************************************************
 *  Initiate the corresponding zookeeper and retrieve the message of the corresponding topic from kafka broker
 *  and parse the message as needed and stores in the <current project directory>/<topic name>_yyyy-MM-dd.csv
 *  where each topic will have one csv file for a day and gets updated as the messages are passed from broker.

 *  @author: Ravi Sankar Karuturi
 *  @createddate: 12/17/2015 
 *****************************************************************************************************/

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

 public class MainConsumer {
 private final ConsumerConnector consumer;
 //private final String topic;

 public MainConsumer(String zookeeper, String groupId)  {
  
  consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper,groupId));
  //this.topic = topic;
 }

 private static ConsumerConfig createConsumerConfig(String zookeeper,String groupId) {
	 Properties props = new Properties();
	props.put("zookeeper.connect", zookeeper);
	props.put("group.id", groupId);
	props.put("zookeeper.session.timeout.ms", "500");
	props.put("zookeeper.sync.time.ms", "250");
	props.put("auto.commit.interval.ms", "1000");


  return new ConsumerConfig(props);

 }

 public String testConsumer(String topic) throws IOException {
    
  String hiveinputmsg= "";
  
  
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
     System.out.println("New message received:: " + gct_msg+ " for topic:: "+topic);
     //msg=gct_msg;
     if(gct_msg != null)
    	 
     hiveinputmsg = (topic.equals( "LogMessaging")) ? buildHiveInput(gct_msg):gct_msg;
             
     Format formatter = new SimpleDateFormat("yyyy-MM-dd");
     Date date = new Date();
     String s = formatter.format(date.getTime());
     String str1=s.replaceAll(" ", "-").replaceAll(":", "-");
     
     
     File currentDirFile = new File(".");
	 String helper = currentDirFile.getAbsolutePath().replace("\\.", "");
     String str = helper+"/out/"+topic+"_"+str1+".csv";
     BufferedWriter out = new BufferedWriter(new FileWriter(str, true));
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
       FinalValue+=GCTValue+","; 
      }
      else
       FinalValue=GCTValue+","; 
      
     }
     FinalValue=FinalValue+"ABC,"+currentdate("yyyy-MM-dd hh.mm.ss.sss")+","+currentdate("yyyy-MM-dd hh.mm.ss.sss")+",1";
     System.out.println("Value for Hive: "+FinalValue);
  
  return FinalValue;
 }
public static String currentdate(String format){

	Date date = new Date();
	String modifiedDate= new SimpleDateFormat(format).format(date);
	return modifiedDate;
}
 
}