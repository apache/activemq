/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.usecases;

import java.util.ArrayList;
import java.util.List;

import javax.jms.ConnectionFactory;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JDBCSpringTest extends TestCase {	
	
	private static Log log = LogFactory.getLog(JDBCSpringTest.class);
	
      int numberOfConsumerThreads = 20;
      int numberOfProducerThreads = 20;
      int numberOfMessages = 50;
      int numberOfQueues = 5;
      String url = "tcp://localhost:61616";
	
      BrokerService broker;
      
    public void setUp() throws Exception {
    	broker = BrokerFactory.createBroker("xbean:activemq-spring-jdbc.xml");
    	broker.start();
    	broker.waitUntilStarted();
    }
    
    
	protected void tearDown() throws Exception {
		broker.stop();
		broker.waitUntilStopped();
	}


	public void testJDBCSpringTest() throws Exception {
		log.info("Using " + numberOfConsumerThreads + " consumers, " + 
				numberOfProducerThreads + " producers, " + 
				numberOfMessages + " messages per publisher, and " +
				numberOfQueues + " queues.");
		
		ConnectionFactory connectionFactory;

		ActiveMQPrefetchPolicy prefetch = new ActiveMQPrefetchPolicy();
		prefetch.setQueuePrefetch(1);
		ActiveMQConnectionFactory	amq = new ActiveMQConnectionFactory(url);
		amq.setPrefetchPolicy(prefetch);
	  
		connectionFactory = new PooledConnectionFactory(amq);
		((PooledConnectionFactory)connectionFactory).setMaxConnections(5);


		StringBuffer buffer = new StringBuffer();		
		for (int i=0; i<2048; i++) {
			buffer.append(".");
		}		
		String twoKbMessage = buffer.toString();
		
		List<ProducerThread> ProducerThreads = new ArrayList<ProducerThread>();
		for (int i=0; i<numberOfProducerThreads; i++) {
			ProducerThread thread = new ProducerThread();
			thread.setMessage(twoKbMessage);
			thread.setNumberOfMessagesToSend(numberOfMessages);
			thread.setNumberOfQueues(numberOfQueues);
			thread.setQueuePrefix("AMQ-2436.queue.");
			thread.setConnectionFactory(connectionFactory);
			//thread.setSendDelay(100);
			ProducerThreads.add(thread);
		}
		
		List<Thread> ConsumerThreads = new ArrayList<Thread>();
		for (int i=0; i<numberOfConsumerThreads; i++) {
			ConsumerThread thread = new ConsumerThread();
			MessageDrivenPojo mdp1 = new MessageDrivenPojo();
			thread.setMessageDrivenPojo(mdp1);
			thread.setConcurrentConsumers(1);
			thread.setConnectionFactory(connectionFactory);
			thread.setDestination("AMQ-2436.queue.");
			thread.setPubSubDomain(false);
			thread.setSessionTransacted(true);
			thread.setNumberOfQueues(numberOfQueues);
			thread.setConsumerName("consumer" + i);
			ConsumerThreads.add(thread);
			thread.start();
		}
		
		
		for (ProducerThread thread : ProducerThreads) {
			thread.start();
		}
		
		boolean finished = false;	
		int previous = 0;
		while (!finished) {
                    
			int totalMessages = 0;	
			for (Thread thread : ConsumerThreads) {
				totalMessages += ((ConsumerThread)thread).getMessageDrivenPojo().getMessageCount();
			}
			log.info(totalMessages + " received so far...");
			if (totalMessages != 0 && previous == totalMessages) {
				for (Thread thread : ConsumerThreads) {
					((ConsumerThread)thread).setRun(false);
				}
				fail("Received " + totalMessages + ", expected " + (numberOfMessages * numberOfProducerThreads));
			}
			previous = totalMessages;
			
			if (totalMessages >= (numberOfMessages * numberOfProducerThreads)) {
				finished = true;
				log.info("Received all " + totalMessages + " messages. Finishing.");
				
				for (Thread thread : ConsumerThreads) {
					((ConsumerThread)thread).setRun(false);
				}
				for (Thread thread : ConsumerThreads) {
					thread.join();
				}

			} else {
				Thread.sleep(1000);
			}
		}
	}	
    
}
