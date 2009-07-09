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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
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
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ExpiredMessagesWithNoConsumerTest extends CombinationTestSupport {

    private static final Log LOG = LogFactory.getLog(ExpiredMessagesWithNoConsumerTest.class);

    private static final int expiryPeriod = 1000;
    
	BrokerService broker;
	Connection connection;
	Session session;
	MessageProducer producer;
	public ActiveMQDestination destination = new ActiveMQQueue("test");
	
    public static Test suite() {
        return suite(ExpiredMessagesWithNoConsumerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
	
    protected void createBrokerWithMemoryLimit() throws Exception {
        doCreateBroker(true);
    }
    
    protected void createBroker() throws Exception {
        doCreateBroker(false);
    }
    
    private void doCreateBroker(boolean memoryLimit) throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("localhost");
        broker.setDataDirectory("data/");
        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.addConnector("tcp://localhost:61616");

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setExpireMessagesPeriod(expiryPeriod);
        defaultEntry.setMaxExpirePageSize(200);

        if (memoryLimit) {
            // so memory is not consumed by DLQ turn if off
            defaultEntry.setDeadLetterStrategy(null);
            defaultEntry.setMemoryLimit(200 * 1000);
        }

        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);

        broker.start();

        broker.waitUntilStarted();
    }
		
	public void testExpiredMessagesWithNoConsumer() throws Exception {
		
	    createBrokerWithMemoryLimit();
	    
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = factory.createConnection();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		producer = session.createProducer(destination);
		producer.setTimeToLive(100);
		connection.start();
		final long sendCount = 2000;		
		
		Thread producingThread = new Thread("Producing Thread") {
            public void run() {
                try {
                	int i = 0;
                	long tStamp = System.currentTimeMillis();
                	while (i++ < sendCount) {
                		producer.send(session.createTextMessage("test"));
                		if (i%100 == 0) {
                		    LOG.info("sent: " + i + " @ " + ((System.currentTimeMillis() - tStamp) / 100)  + "m/ms");
                		    tStamp = System.currentTimeMillis() ;
                		}
                	}
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
		};
		
		producingThread.start();
		
		final long expiry = System.currentTimeMillis() + 20*1000;
		while (producingThread.isAlive() && expiry > System.currentTimeMillis()) {
		    producingThread.join(1000);
		}
        
		assertTrue("producer completed within time ", !producingThread.isAlive());
		
		Thread.sleep(3*expiryPeriod);
        DestinationViewMBean view = createView(destination);
        assertEquals("All sent have expired ", sendCount, view.getExpiredCount());
	}

	
    
    public void testExpiredMessagesWitVerySlowConsumer() throws Exception {
        createBroker();  
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        connection = factory.createConnection();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        producer = session.createProducer(destination);
        final int ttl = 4000;
        producer.setTimeToLive(ttl);
        
        final long sendCount = 1001; 
        final CountDownLatch receivedOneCondition = new CountDownLatch(1);
        final CountDownLatch waitCondition = new CountDownLatch(1);
        
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {

            public void onMessage(Message message) {
                try {
                    LOG.info("Got my message: " + message);
                    receivedOneCondition.countDown();
                    waitCondition.await(60, TimeUnit.SECONDS);
                    message.acknowledge();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.toString());
                }  
            }        
        });
        
        connection.start();
      
        
        Thread producingThread = new Thread("Producing Thread") {
            public void run() {
                try {
                    int i = 0;
                    long tStamp = System.currentTimeMillis();
                    while (i++ < sendCount) {
                        producer.send(session.createTextMessage("test"));
                        if (i%100 == 0) {
                            LOG.info("sent: " + i + " @ " + ((System.currentTimeMillis() - tStamp) / 100)  + "m/ms");
                            tStamp = System.currentTimeMillis() ;
                        }
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };
        
        producingThread.start();
        
        final long expiry = System.currentTimeMillis() + 20*1000;
        while (producingThread.isAlive() && expiry > System.currentTimeMillis()) {
            producingThread.join(1000);
        }
        
        assertTrue("got one message", receivedOneCondition.await(10, TimeUnit.SECONDS));
        assertTrue("producer completed within time ", !producingThread.isAlive());
        
        Thread.sleep(2 * Math.max(ttl, expiryPeriod));
        DestinationViewMBean view = createView(destination);
            
        assertEquals("all dispatched up to default prefetch ", 1000, view.getDispatchCount());
        assertEquals("All sent save one have expired ", sendCount, view.getExpiredCount());     
        
        
        // let the ack happen
        waitCondition.countDown();
     
        Thread.sleep(Math.max(ttl, expiryPeriod));
        
        assertEquals("all sent save one have expired ", sendCount, view.getExpiredCount());
        
        assertEquals("prefetch gets back to 0 ", 0, view.getInFlightCount());
        
        consumer.close();
        LOG.info("done: " + getName());
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
