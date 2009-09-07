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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import junit.framework.Test;


public class ExpiredMessagesWithNoConsumerTest extends CombinationTestSupport {

    private static final Log LOG = LogFactory.getLog(ExpiredMessagesWithNoConsumerTest.class);

    
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
        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.addConnector("tcp://localhost:61616");

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        // TODO Optimize dispatch makes this test hang
        //defaultEntry.setOptimizedDispatch(true);
        defaultEntry.setExpireMessagesPeriod(100);
        defaultEntry.setMaxExpirePageSize(800);

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
		producer.setTimeToLive(1000);
		connection.start();
		final long sendCount = 2000;		
		
		final Thread producingThread = new Thread("Producing Thread") {
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
		
		assertTrue("producer completed within time", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                producingThread.join(1000);
                return !producingThread.isAlive();
            }
		}));
		
        final DestinationViewMBean view = createView(destination);
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return sendCount == view.getExpiredCount();
            }
        }, 5*1000l);
        LOG.info("enqueue=" + view.getEnqueueCount() + ", dequeue=" + view.getDequeueCount()
                + ", inflight=" + view.getInFlightCount() + ", expired= " + view.getExpiredCount()
                + ", size= " + view.getQueueSize());
        assertEquals("All sent have expired", sendCount, view.getExpiredCount());
	}
    
	// first ack delivered after expiry
    public void testExpiredMessagesWithVerySlowConsumer() throws Exception {
        createBroker();  
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        connection = factory.createConnection();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        producer = session.createProducer(destination);
        final int ttl = 4000;
        producer.setTimeToLive(ttl);
        
        final long sendCount = 1500; 
        final CountDownLatch receivedOneCondition = new CountDownLatch(1);
        final CountDownLatch waitCondition = new CountDownLatch(1);
        
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {

            public void onMessage(Message message) {
                try {
                    LOG.info("Got my message: " + message);
                    receivedOneCondition.countDown();
                    waitCondition.await(60, TimeUnit.SECONDS);
                    LOG.info("acking message: " + message);
                    message.acknowledge();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.toString());
                }  
            }        
        });
        
        connection.start();
      
        
        final Thread producingThread = new Thread("Producing Thread") {
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
        assertTrue("got one message", receivedOneCondition.await(20, TimeUnit.SECONDS));
        
        assertTrue("producer completed within time ", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                producingThread.join(1000);
                return !producingThread.isAlive();
            }      
        }));
             
        final DestinationViewMBean view = createView(destination);
            
        assertTrue("all dispatched up to default prefetch ", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return 1000 == view.getDispatchCount();
            }
        }));
        assertTrue("All sent have expired ", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return sendCount == view.getExpiredCount();
            }
        }));     
        
        LOG.info("enqueue=" + view.getEnqueueCount() + ", dequeue=" + view.getDequeueCount()
                + ", inflight=" + view.getInFlightCount() + ", expired= " + view.getExpiredCount()
                + ", size= " + view.getQueueSize());
        
        // let the ack happen
        waitCondition.countDown();
        
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return 0 == view.getInFlightCount();
            }
        });
        LOG.info("enqueue=" + view.getEnqueueCount() + ", dequeue=" + view.getDequeueCount()
                + ", inflight=" + view.getInFlightCount() + ", expired= " + view.getExpiredCount()
                + ", size= " + view.getQueueSize());
        assertEquals("prefetch gets back to 0 ", 0, view.getInFlightCount());
        assertEquals("size gets back to 0 ", 0, view.getQueueSize());
        assertEquals("dequeues match sent/expired ", sendCount, view.getDequeueCount());
        
        consumer.close();
        LOG.info("done: " + getName());
    }

	protected DestinationViewMBean createView(ActiveMQDestination destination) throws Exception {
        String domain = "org.apache.activemq";
        ObjectName name;
        if (destination.isQueue()) {
            name = new ObjectName(domain + ":BrokerName=localhost,Type=Queue,Destination=test");
        } else {
            name = new ObjectName(domain + ":BrokerName=localhost,Type=Topic,Destination=test");
        }
        return (DestinationViewMBean) broker.getManagementContext().newProxyInstance(name, DestinationViewMBean.class,
                true);
    }

	protected void tearDown() throws Exception {
		connection.stop();
		broker.stop();
		broker.waitUntilStopped();
	}

	

	
}
