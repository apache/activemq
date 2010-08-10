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
package org.apache.activemq.bugs;


import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class JmsTimeoutTest extends EmbeddedBrokerTestSupport {

		static final Log LOG = LogFactory.getLog(JmsTimeoutTest.class);
	
		private int messageSize=1024*64;
		private int messageCount=10000;
		private final AtomicInteger exceptionCount = new AtomicInteger(0);
		
	    /**
	     * Test the case where the broker is blocked due to a memory limit 
	     * and a producer timeout is set on the connection.
	     * @throws Exception
	     */
	    public void testBlockedProducerConnectionTimeout() throws Exception {
	        final ActiveMQConnection cx = (ActiveMQConnection)createConnection();
	        final ActiveMQDestination queue = createDestination("testqueue");
	        
	        // we should not take longer than 5 seconds to return from send
	        cx.setSendTimeout(10000);
	        	
	        Runnable r = new Runnable() {
	            public void run() {
	                try {
	                	LOG.info("Sender thread starting");
	                    Session session = cx.createSession(false, 1);
	                    MessageProducer producer = session.createProducer(queue);
	                    producer.setDeliveryMode(DeliveryMode.PERSISTENT);
	                    
	                    TextMessage message = session.createTextMessage(createMessageText());
	                    for(int count=0; count<messageCount; count++){
	                    	producer.send(message);
	                    	// Currently after the timeout producer just
	                    	// returns but there is no way to know that
	                    	// the send timed out
	                    }	  
	                    LOG.info("Done sending..");
	                } catch (JMSException e) {
	                    e.printStackTrace();
	                    if (e instanceof ResourceAllocationException) {
	                        exceptionCount.incrementAndGet();
	                    }
	                    return;
	                }
	            }
	        };
	        cx.start();
	        Thread producerThread = new Thread(r);
	        producerThread.start();
	        producerThread.join(30000);
	        cx.close();
	        // We should have a few timeout exceptions as memory store will fill up
	        assertTrue("No exception from the broker", exceptionCount.get() > 0);
	    }

	    protected void setUp() throws Exception {
	        bindAddress = "tcp://localhost:61616";
	        broker = createBroker();
	        broker.setDeleteAllMessagesOnStartup(true);
	        broker.getSystemUsage().getMemoryUsage().setLimit(5*1024*1024);
	        broker.getSystemUsage().setSendFailIfNoSpaceAfterTimeout(5000);
	        super.setUp();
	    }

        private String createMessageText() {
	        StringBuffer buffer = new StringBuffer();
	        buffer.append("<filler>");
	        for (int i = buffer.length(); i < messageSize; i++) {
	            buffer.append('X');
	        }
	        buffer.append("</filler>");
	        return buffer.toString();
	    }
	    
	}