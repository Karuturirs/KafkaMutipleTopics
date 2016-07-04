package com.broker.kafka.multipletopic;

/****************************************************************************************************
 *  Starts the producer to create messages random for the topics "LogMessaging","UserLogin","ProductOrderDetail","Ads"
 *  example messages for each topic:
 *    LogMessaging:::gctCookieId=2015-12-17/08.37.57.057-b6a873ca-2f0c-9f03-323b-e319788f25ec, fullUrl=https://www.myecommers.com/us/offers/2644?extlink=ps-us-ccsg-google_VA_American_Express_Exact_RLSA_VR_A, eep=null, chName=null,  eventTypeIndicator=app_start, s_Vi=null,  pzn_Model_Score=123
 *    UserLogin:::991546,2015-12-17 08.37.56.056,74.211.235.133,2015-12-17 08.37.56.056,2015-12-17 08.37.56.056,1
 *    Ads:::957755,fLu5PF5Mmk,2015-12-17 08.37.56.056,2015-12-17 08.37.56.056,1
 *    ProductOrderDetail:::950680,JaqxK1bwXr,3,2015-12-17 08.37.55.055,2015-12-17 08.37.55.055,1

 *  @author: Ravi Sankar Karuturi
 *  @createddate: 12/17/2015 
 *****************************************************************************************************/
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import com.broker.kafka.prac.utils.StringRandomGen;



public class StartProducer {
	
	public static void main(String[] args)throws InterruptedException, ExecutionException, IOException {
		
		String[] topicList ={"LogMessaging","UserLogin","ProductOrderDetail","Ads"};
		
		MainProducer obj=new MainProducer("localhost:2181");
		for(int i=1;i<=7;i++){
			int x=randInt(0,topicList.length-1);
			feedMessages(obj,topicList[x],createMessages(topicList[x]));
		}
	
	}
	public static int randInt(int min, int max) {
	    // Usually this can be a field rather than a method variable
	    Random rand = new Random();
	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = rand.nextInt((max - min) + 1) + min;
	    return randomNum;
	}
	
	public static void feedMessages(MainProducer obj,String topic,String newMessage){
		boolean is_msgreceived=false;
		try {
			System.out.println(topic+":::"+newMessage);
			is_msgreceived=obj.constructAndSendMessage(topic,newMessage);
	
		}catch(Exception e) {
			e.printStackTrace();
		}
		System.out.println("Status of the topic "+topic.toUpperCase()+" kafka message...:"+is_msgreceived);
	}
	public static String createMessages(String topic ){
		
		String messagegenerated= "";
		String[] webpages={"pdp","offers","templates","campain"};
		switch (topic) {
	        case "LogMessaging":  messagegenerated = "gctCookieId="+currentdate("yyyy-MM-dd/hh.mm.ss.sss")+"-b6a873ca-2f0c-9f03-323b-e319788f25ec, fullUrl=https://www.myecommers.com/us/"+webpages[randInt(0,3)]+"/"+randInt(2000,9000)+"?extlink=ps-us-ccsg-google_VA_American_Express_Exact_RLSA_VR_A, eep=null, chName=null,  eventTypeIndicator=app_start, s_Vi=null,  pzn_Model_Score=123";
	                 break;
	        case "UserLogin":  messagegenerated = randInt(900000,999999)+","+currentdate("yyyy-MM-dd hh.mm.ss.sss")+","+ipGeneration()+","+currentdate("yyyy-MM-dd hh.mm.ss.sss")+","+currentdate("yyyy-MM-dd hh.mm.ss.sss")+",1";
	                 break;
	        case "ProductOrderDetail":  messagegenerated = randInt(900000,999999)+","+randomString()+","+randInt(1,5)+","+currentdate("yyyy-MM-dd hh.mm.ss.sss")+","+currentdate("yyyy-MM-dd hh.mm.ss.sss")+",1";
	                 break;
	        case "Ads":  messagegenerated = randInt(900000,999999)+","+randomString()+","+currentdate("yyyy-MM-dd hh.mm.ss.sss")+","+currentdate("yyyy-MM-dd hh.mm.ss.sss")+",1";
	                 break;
		}
		return messagegenerated;
	}
	
	public static String currentdate(String format){

		Date date = new Date();
		String modifiedDate= new SimpleDateFormat(format).format(date);
		return modifiedDate;
	}
	public static String ipGeneration(){
		Random r = new Random();
		return r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
	}
	
	public static String randomString(){
		 StringRandomGen msr = new StringRandomGen();
	       return msr.generateRandomString();
	}

}
