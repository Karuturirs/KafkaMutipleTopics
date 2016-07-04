package com.broker.kafka.prac.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import com.broker.kafka.prac.JsonConsumer;

public class ConsumeTestCase {

	@Test
	public void test()
		throws InterruptedException, ExecutionException, IOException{
		JsonConsumer jc = new JsonConsumer("localhost:2181", "mykey","kafkatopic");
		assertEquals("New message received:: gctcookieid: 123, full url: www.google.com", jc.testConsumer());
	}

}