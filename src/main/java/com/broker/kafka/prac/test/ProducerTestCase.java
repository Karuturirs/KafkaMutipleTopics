package com.broker.kafka.prac.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import com.broker.kafka.prac.JsonProducer;

public class ProducerTestCase {

	@Test
	public void test() 
		throws InterruptedException, ExecutionException, IOException{
		
		JsonProducer obj=new JsonProducer("localhost:2181", "kafkatopic");
		obj.constructAndSendMessage("kafkatopic","mykey");
		assertNotNull(obj);
		
	}

}
