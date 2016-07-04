
package com.broker.kafka.prac.test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.broker.kafka.prac.JsonConsumer;
import com.broker.kafka.prac.utils.PropertyFileReader;

public class TestConsumer {

 public static void main(String[] args) throws InterruptedException, ExecutionException, IOException { 
	 PropertyFileReader propload =new PropertyFileReader();
  JsonConsumer jc = new JsonConsumer(propload.getProperty("Host")+":"+propload.getProperty("ProducerPort"), "gct_GroupId",propload.getProperty("Topic"));
  jc.testConsumer();
 }
}

