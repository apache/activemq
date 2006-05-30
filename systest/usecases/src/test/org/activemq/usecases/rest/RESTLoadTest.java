package org.activemq.usecases.rest;

import junit.framework.*;

public class RESTLoadTest extends TestCase {
    public volatile int counter = 0;

    public void testREST() {
        int HowManyMessages = 60000;
        TestConsumerThread consumer = new TestConsumerThread(this, HowManyMessages);
        TestProducerThread producer = new TestProducerThread(this, HowManyMessages);
        consumer.start();
        producer.start();
        while (counter > 0) {
        }
        System.out.println("Produced:" + producer.success + " Consumed:" + consumer.success);
    }

    public static void main(String args[]) {
        junit.textui.TestRunner.run(new TestSuite(RESTLoadTest.class));
    }
}
 
