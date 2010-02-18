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

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.amq.AMQPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.apache.activemq.TestSupport.getDestination;
import static org.apache.activemq.TestSupport.getDestinationStatistics;


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
        
        final DestinationStatistics view = getDestinationStatistics(broker, destination);

        // wait for all to inflight to expire
        assertTrue("all inflight messages expired ", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return view.getInflight().getCount() == 0;
            }        
        }));
        assertEquals("Wrong inFlightCount: ", 0, view.getInflight().getCount());
        
        LOG.info("Stats: received: "  + received.get() + ", enqueues: " + view.getEnqueues().getCount() + ", dequeues: " + view.getDequeues().getCount()
                + ", dispatched: " + view.getDispatched().getCount() + ", inflight: " + view.getInflight().getCount() + ", expiries: " + view.getExpired().getCount());
        
        // wait for all sent to get delivered and expire
        assertTrue("all sent messages expired ", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                long oldEnqueues = view.getEnqueues().getCount();
                Thread.sleep(200);
                LOG.info("Stats: received: "  + received.get() + ", size= " + view.getMessages().getCount() + ", enqueues: " + view.getDequeues().getCount() + ", dequeues: " + view.getDequeues().getCount()
                        + ", dispatched: " + view.getDispatched().getCount() + ", inflight: " + view.getInflight().getCount() + ", expiries: " + view.getExpired().getCount());
                return oldEnqueues == view.getEnqueues().getCount();
            }           
        }, 60*1000));
        

        LOG.info("Stats: received: "  + received.get() + ", size= " + view.getMessages().getCount() + ", enqueues: " + view.getEnqueues().getCount() + ", dequeues: " + view.getDequeues().getCount()
                + ", dispatched: " + view.getDispatched().getCount() + ", inflight: " + view.getInflight().getCount() + ", expiries: " + view.getExpired().getCount());
        
        assertTrue("got at least what did not expire", received.get() >= view.getDequeues().getCount() - view.getExpired().getCount());
        
        assertTrue("all messages expired - queue size gone to zero " + view.getMessages().getCount(), Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                LOG.info("Stats: received: "  + received.get() + ", size= " + view.getMessages().getCount() + ", enqueues: " + view.getEnqueues().getCount() + ", dequeues: " + view.getDequeues().getCount()
                        + ", dispatched: " + view.getDispatched().getCount() + ", inflight: " + view.getInflight().getCount() + ", expiries: " + view.getExpired().getCount());
                return view.getMessages().getCount() == 0;
            }
        }));
        
        final long expiredBeforeEnqueue = numMessagesToSend - view.getEnqueues().getCount();
        final long totalExpiredCount = view.getExpired().getCount() + expiredBeforeEnqueue;
        
        final DestinationStatistics dlqView = getDestinationStatistics(broker, dlqDestination);
        LOG.info("DLQ stats: size= " + dlqView.getMessages().getCount() + ", enqueues: " + dlqView.getDequeues().getCount() + ", dequeues: " + dlqView.getDequeues().getCount()
                + ", dispatched: " + dlqView.getDispatched().getCount() + ", inflight: " + dlqView.getInflight().getCount() + ", expiries: " + dlqView.getExpired().getCount());
        
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return totalExpiredCount == dlqView.getMessages().getCount();
            }
        });
        assertEquals("dlq contains all expired", totalExpiredCount, dlqView.getMessages().getCount());
        
        // memory check
        assertEquals("memory usage is back to duck egg", 0, getDestination(broker, destination).getMemoryUsage().getPercentUsage());
        assertTrue("memory usage is increased ", 0 < getDestination(broker, dlqDestination).getMemoryUsage().getPercentUsage());    
        
        // verify DLQ
        MessageConsumer dlqConsumer = createDlqConsumer(connection);
        final DLQListener dlqListener = new DLQListener();
        dlqConsumer.setMessageListener(dlqListener);
        
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return totalExpiredCount == dlqListener.count;
            }
        }, 60 * 1000);
        
        assertEquals("dlq returned all expired", dlqListener.count, totalExpiredCount);
	}

    class DLQListener implements MessageListener {
        
        int count = 0;
        
        public void onMessage(Message message) {
            count++;
        }
        
    };
    
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

        DestinationStatistics view = getDestinationStatistics(broker, destination);
        LOG.info("Stats: size: " + view.getMessages().getCount() + ", enqueues: " 
                + view.getEnqueues().getCount() + ", dequeues: "
                + view.getDequeues().getCount() + ", dispatched: "
                + view.getDispatched().getCount() + ", inflight: "
                + view.getInflight().getCount() + ", expiries: "
                + view.getExpired().getCount());

        LOG.info("stopping broker");
        broker.stop();
        broker.waitUntilStopped();

        Thread.sleep(5000);

        LOG.info("recovering broker");
        final boolean deleteAllMessages = false;
        broker = createBroker(deleteAllMessages, 5000);
        
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                DestinationStatistics view = getDestinationStatistics(broker, destination);
                LOG.info("Stats: size: " + view.getMessages().getCount() + ", enqueues: "
                        + view.getEnqueues().getCount() + ", dequeues: "
                        + view.getDequeues().getCount() + ", dispatched: "
                        + view.getDispatched().getCount() + ", inflight: "
                        + view.getInflight().getCount() + ", expiries: "
                        + view.getExpired().getCount());
                    
                return view.getMessages().getCount() == 0;
            }
        });
        
        view = getDestinationStatistics(broker, destination);
        assertEquals("Expect empty queue, QueueSize: ", 0, view.getMessages().getCount());
        assertEquals("all dequeues were expired", view.getDequeues().getCount(), view.getExpired().getCount());
    }

	private BrokerService createBroker(boolean deleteAllMessages, long expireMessagesPeriod) throws Exception {
	    BrokerService broker = new BrokerService();
        broker.setBrokerName("localhost");
        broker.setDestinations(new ActiveMQDestination[]{destination});
        AMQPersistenceAdapter adaptor = new AMQPersistenceAdapter();
        adaptor.setDirectory(new File("target/expiredtest-data/"));
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
    
    

	protected void tearDown() throws Exception {
		connection.stop();
		broker.stop();
		broker.waitUntilStopped();
	}
}
