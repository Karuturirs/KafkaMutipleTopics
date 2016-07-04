package com.broker.kafka.multipletopic;

/****************************************************************************************************
 *  Starts the Consumer to fetch all the topics messages that are passed from producer in concurrent methodology. 

 *  @author: Ravi Sankar Karuturi
 *  @createddate: 12/17/2015 
 *****************************************************************************************************/

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;




public class StartConsumer {
		private static final int MYTHREADS = 19;
		 
	 public static void main(String[] args) throws InterruptedException, ExecutionException, IOException { 
		 ExecutorService executor = Executors.newFixedThreadPool(MYTHREADS);
		
	     String[] topicList ={"LogMessaging","UserLogin","ProductOrderDetail","Ads"};
	     System.out.println("Starting Consumer");
		   for (int i = 0; i < topicList.length; i++) {
			   
				String topic = topicList[i];
				Runnable worker = new MyRunnable(topic);
				executor.execute(worker);
			}
			executor.shutdown();
			// Wait until all threads are finish
			while (!executor.isTerminated()) {
		
			}
			System.out.println("\nFinished all threads");
			System.out.println("Ending Consumer");
	 }
	 public static class MyRunnable implements Runnable {
		private final String topic;

		MyRunnable(String topic) {
			this.topic = topic;
		}

		@Override
		public void run() {

			try {
				MainConsumer jc = new MainConsumer("localhost:2181", "gct_GroupId");
				jc.testConsumer(topic);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println( "\t\tStatus:" );
		}
	}
}
