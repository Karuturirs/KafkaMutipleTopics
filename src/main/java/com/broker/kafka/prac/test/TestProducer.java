package com.broker.kafka.prac.test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.broker.kafka.prac.JsonProducer;
import com.broker.kafka.prac.utils.PropertyFileReader;

public class TestProducer {

	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {	
		
		boolean is_msgreceived=false;
		PropertyFileReader propload =new PropertyFileReader();
		try {
		String newMessage = "gctCookieId=2015-10-19/15:43:15:151-b6a873ca-2f0c-9f03-323b-e319788f25ec, fullUrl=https://www.americanexpress.com/us/credit-cards/28009?extlink=ps-us-ccsg-google_VA_American_Express_Exact_RLSA_VR_A, eep=null, chName=null, bUnit=null, paidSearchEng=null, paidSearchKey=null, affiliatePatner=null, naturalSearchChannelInd=null, naturalSearchEng=null, refSite=null, pGuid=null, eventTypeIndicator=app_start, s_Vi=null, ipAddress=null, adobeCookieId=null, pcn=null, clientTimeStamp=null, userAgent=null, applyFlowId=null, prodCode=null, zipCode=null, prodSku=null, uuid=null, event_Num=null, pzn_Model_Score=123, pzn_Seg=null, src_Cd=null";
		String topic=propload.getProperty("Topic");
		JsonProducer obj=new JsonProducer(propload.getProperty("Host")+":"+propload.getProperty("ProducerPort"),topic);
		is_msgreceived=obj.constructAndSendMessage(topic,newMessage);
	
		}
		catch(Exception e) {}
		System.out.println("status of the kafka message..."+is_msgreceived);
  }
}
	
