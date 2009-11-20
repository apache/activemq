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
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.amq.AMQPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.File;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;
import junit.framework.Test;

public class ExpiredMessagesTest extends CombinationTestSupport {

    private static final Log LOG = LogFactory.getLog(ExpiredMessagesTest.class);
    
    BrokerService broker;
    Connection connection;
    Session session;
    MessageProducer producer;
    MessageConsumer consumer;
    public ActiveMQDestination destination = new ActiveMQQueue("test");
    public ActiveMQDestination dlqDestination = new ActiveMQQueue("ActiveMQ.DLQ");
    public boolean useTextMessage = true;
    public boolean useVMCursor = true;
    
    public static Test suite() {
        return suite(ExpiredMessagesTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
	
	protected void setUp() throws Exception {
        final boolean deleteAllMessages = true;
        broker = createBroker(deleteAllMessages, 100);
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
					consumer.close();
				} catch (Throwable ex) {
					ex.printStackTrace();
				}
			}
		};
		
        consumerThread.start();
		
		final int numMessagesToSend = 10000;
		Thread producingThread = new Thread("Producing Thread") {
            public void run() {
                try {
                	int i = 0;
                	while (i++ < numMessagesToSend) {
                		producer.send(session.createTextMessage("test"));
                	}
                	producer.close();
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
		};
		
		producingThread.start();
		
        consumerThread.join();
        producingThread.join();
        session.close();
        
        final DestinationViewMBean view = createView(destination);
        
        // wait for all to inflight to expire
        assertTrue("all inflight messages expired ", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return view.getInFlightCount() == 0;
            }           
        }));
        assertEquals("Wrong inFlightCount: ", 0, view.getInFlightCount());
        
        LOG.info("Stats: received: "  + received.get() + ", enqueues: " + view.getDequeueCount() + ", dequeues: " + view.getDequeueCount()
                + ", dispatched: " + view.getDispatchCount() + ", inflight: " + view.getInFlightCount() + ", expiries: " + view.getExpiredCount());
        
        // wait for all sent to get delivered and expire
        assertTrue("all sent messages expired ", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                long oldEnqueues = view.getEnqueueCount();
                Thread.sleep(200);
                LOG.info("Stats: received: "  + received.get() + ", size= " + view.getQueueSize() + ", enqueues: " + view.getDequeueCount() + ", dequeues: " + view.getDequeueCount()
                        + ", dispatched: " + view.getDispatchCount() + ", inflight: " + view.getInFlightCount() + ", expiries: " + view.getExpiredCount());
                return oldEnqueues == view.getEnqueueCount();
            }           
        }, 60*1000));
        

        LOG.info("Stats: received: "  + received.get() + ", size= " + view.getQueueSize() + ", enqueues: " + view.getDequeueCount() + ", dequeues: " + view.getDequeueCount()
                + ", dispatched: " + view.getDispatchCount() + ", inflight: " + view.getInFlightCount() + ", expiries: " + view.getExpiredCount());
        
        assertTrue("got at least what did not expire", received.get() >= view.getDequeueCount() - view.getExpiredCount());
        
        assertTrue("all messages expired - queue size gone to zero " + view.getQueueSize(), Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                LOG.info("Stats: received: "  + received.get() + ", size= " + view.getQueueSize() + ", enqueues: " + view.getDequeueCount() + ", dequeues: " + view.getDequeueCount()
                        + ", dispatched: " + view.getDispatchCount() + ", inflight: " + view.getInFlightCount() + ", expiries: " + view.getExpiredCount());
                return view.getQueueSize() == 0;
            }
        }));
        
        final long expiredBeforeEnqueue = numMessagesToSend - view.getEnqueueCount();
        final long totalExpiredCount = view.getExpiredCount() + expiredBeforeEnqueue;
        
        final DestinationViewMBean dlqView = createView(dlqDestination);
        LOG.info("DLQ stats: size= " + dlqView.getQueueSize() + ", enqueues: " + dlqView.getDequeueCount() + ", dequeues: " + dlqView.getDequeueCount()
                + ", dispatched: " + dlqView.getDispatchCount() + ", inflight: " + dlqView.getInFlightCount() + ", expiries: " + dlqView.getExpiredCount());
        
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return totalExpiredCount == dlqView.getQueueSize();
            }
        });
        assertEquals("dlq contains all expired", totalExpiredCount, dlqView.getQueueSize());
        
        // memory check
        assertEquals("memory usage is back to duck egg", 0, view.getMemoryPercentUsage());
        assertTrue("memory usage is increased ", 0 < dlqView.getMemoryPercentUsage());    
        
        // verify DQL
        MessageConsumer dlqConsumer = createDlqConsumer(connection);
        int count = 0;
        while (dlqConsumer.receive(4000) != null) {
            count++;
        }
        assertEquals("dlq returned all expired", count, totalExpiredCount);
	}

	
	private MessageConsumer createDlqConsumer(Connection connection) throws Exception {
	    return connection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(dlqDestination);
    }

    public void initCombosForTestRecoverExpiredMessages() {
	    addCombinationValues("useVMCursor", new Object[] {Boolean.TRUE, Boolean.FALSE});
	}
	
	public void testRecoverExpiredMessages() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                "failover://tcp://localhost:61616");
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(destination);
        producer.setTimeToLive(2000);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        Thread producingThread = new Thread("Producing Thread") {
            public void run() {
                try {
                    int i = 0;
                    while (i++ < 1000) {
                        Message message = useTextMessage ? session
                                .createTextMessage("test") : session
                                .createObjectMessage("test");
                        producer.send(message);
                    }
                    producer.close();
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };

        producingThread.start();
        producingThread.join();

        DestinationViewMBean view = createView(destination);
        LOG.info("Stats: size: " + view.getQueueSize() + ", enqueues: "
                + view.getDequeueCount() + ", dequeues: "
                + view.getDequeueCount() + ", dispatched: "
                + view.getDispatchCount() + ", inflight: "
                + view.getInFlightCount() + ", expiries: "
                + view.getExpiredCount());

        LOG.info("stopping broker");
        broker.stop();
        broker.waitUntilStopped();

        Thread.sleep(5000);

        LOG.info("recovering broker");
        final boolean deleteAllMessages = false;
        broker = createBroker(deleteAllMessages, 5000);
        
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                boolean result = false;
                try {
                    DestinationViewMBean view = createView(destination);
                    LOG.info("Stats: size: " + view.getQueueSize() + ", enqueues: "
                            + view.getDequeueCount() + ", dequeues: "
                            + view.getDequeueCount() + ", dispatched: "
                            + view.getDispatchCount() + ", inflight: "
                            + view.getInFlightCount() + ", expiries: "
                            + view.getExpiredCount());

                    result = view.getQueueSize() == 0;
                } catch (InstanceNotFoundException expectedOnSlowMachines) {
                }
                return result;
            }
        });
        
        view = createView(destination);
        assertEquals("Expect empty queue, QueueSize: ", 0, view.getQueueSize());
        assertEquals("all dequeues were expired", view.getDequeueCount(), view.getExpiredCount());
    }

	private BrokerService createBroker(boolean deleteAllMessages, long expireMessagesPeriod) throws Exception {
	    BrokerService broker = new BrokerService();
        broker.setBrokerName("localhost");
        AMQPersistenceAdapter adaptor = new AMQPersistenceAdapter();
        adaptor.setDirectory(new File("data/"));
        adaptor.setForceRecoverReferenceStore(true);
        broker.setPersistenceAdapter(adaptor);
        
        PolicyEntry defaultPolicy = new PolicyEntry();
        if (useVMCursor) {
            defaultPolicy.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        }
        defaultPolicy.setExpireMessagesPeriod(expireMessagesPeriod);
        defaultPolicy.setMaxExpirePageSize(1200);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(defaultPolicy);
        broker.setDestinationPolicy(policyMap);
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        broker.addConnector("tcp://localhost:61616");
        broker.start();
        broker.waitUntilStarted();
        return broker;
    }
	
    protected DestinationViewMBean createView(ActiveMQDestination destination) throws Exception {
        String domain = "org.apache.activemq";
        ObjectName name;
        if (destination.isQueue()) {
            name = new ObjectName(domain + ":BrokerName=localhost,Type=Queue,Destination="
                    + destination.getPhysicalName());
        } else {
            name = new ObjectName(domain + ":BrokerName=localhost,Type=Topic,Destination="
                    + destination.getPhysicalName());
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
