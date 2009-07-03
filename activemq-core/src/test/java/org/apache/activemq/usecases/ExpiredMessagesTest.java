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

import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



public class ExpiredMessagesTest extends CombinationTestSupport {

    private static final Log LOG = LogFactory.getLog(ExpiredMessagesTest.class);
    
	BrokerService broker;
	Connection connection;
	Session session;
	MessageProducer producer;
	MessageConsumer consumer;
	public ActiveMQDestination destination = new ActiveMQQueue("test");
	
    public static Test suite() {
        return suite(ExpiredMessagesTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
	
	protected void setUp() throws Exception {
		broker = new BrokerService();
		broker.setBrokerName("localhost");
		broker.setDataDirectory("data/");
		broker.setUseJmx(true);
		broker.deleteAllMessages();
		broker.addConnector("tcp://localhost:61616");
		broker.start();
		broker.waitUntilStarted();
	}
	
    public void initCombosForTestExpiredMessages() {
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("test"), new ActiveMQTopic("test")});
    }
	
	public void testExpiredMessages() throws Exception {
		
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = factory.createConnection();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		producer = session.createProducer(destination);
		producer.setTimeToLive(100);
		consumer = session.createConsumer(destination);
		connection.start();
		final AtomicLong received = new AtomicLong();
		
		Thread consumerThread = new Thread("Consumer Thread") {
			public void run() {
				long start = System.currentTimeMillis();
				try {
					long end = System.currentTimeMillis();
					while (end - start < 3000) {
						if (consumer.receive(1000) != null) {
						    received.incrementAndGet();
						}
						Thread.sleep(100);
						end = System.currentTimeMillis();
					}
				} catch (Throwable ex) {
					ex.printStackTrace();
				}
			}
		};
		
        consumerThread.start();
		
		
		Thread producingThread = new Thread("Producing Thread") {
            public void run() {
                try {
                	int i = 0;
                	while (i++ < 30000) {
                		producer.send(session.createTextMessage("test"));
                	}
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
		};
		
		producingThread.start();
		
        consumerThread.join();
        producingThread.join();
        
        
        DestinationViewMBean view = createView(destination);
        LOG.info("Stats: received: "  + received.get() + ", enqueues: " + view.getDequeueCount() + ", dequeues: " + view.getDequeueCount()
                + ", dispatched: " + view.getDispatchCount() + ", inflight: " + view.getInFlightCount() + ", expiries: " + view.getExpiredCount());
        
        assertEquals("got what did not expire", received.get(), view.getDequeueCount() - view.getExpiredCount());
        //assertEquals("Wrong inFlightCount: " + view.getInFlightCount(), view.getDispatchCount() - view.getDequeueCount(), view.getInFlightCount());
	}
	
	protected DestinationViewMBean createView(ActiveMQDestination destination) throws Exception {
		 MBeanServer mbeanServer = broker.getManagementContext().getMBeanServer();
		 String domain = "org.apache.activemq";
		 ObjectName name;
		if (destination.isQueue()) {
			name = new ObjectName(domain + ":BrokerName=localhost,Type=Queue,Destination=test");
		} else {
			name = new ObjectName(domain + ":BrokerName=localhost,Type=Topic,Destination=test");
		}
		return (DestinationViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, name, DestinationViewMBean.class, true);
	}

	protected void tearDown() throws Exception {
		connection.stop();
		broker.stop();
		broker.waitUntilStopped();
	}

	

	
}
