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
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.management.ObjectName;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.region.policy.FilePendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpiredMessagesWithNoConsumerTest extends CombinationTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ExpiredMessagesWithNoConsumerTest.class);

    private final ActiveMQDestination destination = new ActiveMQQueue("test");

    private boolean optimizedDispatch = true;
    private PendingQueueMessageStoragePolicy pendingQueuePolicy;

    private BrokerService broker;
    private String connectionUri;
    private Connection connection;
    private Session session;
    private MessageProducer producer;

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
        broker.addConnector("tcp://localhost:0");

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setOptimizedDispatch(optimizedDispatch);
        defaultEntry.setExpireMessagesPeriod(800);
        defaultEntry.setMaxExpirePageSize(800);

        defaultEntry.setPendingQueuePolicy(pendingQueuePolicy);

        if (memoryLimit) {
            // so memory is not consumed by DLQ turn if off
            defaultEntry.setDeadLetterStrategy(null);
            defaultEntry.setMemoryLimit(200 * 1000);
        }

        policyMap.setDefaultEntry(defaultEntry);

        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    public void initCombosForTestExpiredMessagesWithNoConsumer() {
        addCombinationValues("optimizedDispatch", new Object[] {Boolean.TRUE, Boolean.FALSE});
        addCombinationValues("pendingQueuePolicy", new Object[] {null, new VMPendingQueueMessageStoragePolicy(), new FilePendingQueueMessageStoragePolicy()});
    }

    public void testExpiredMessagesWithNoConsumer() throws Exception {

        createBrokerWithMemoryLimit();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
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

        assertTrue("producer failed to complete within allocated time", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                producingThread.join(TimeUnit.SECONDS.toMillis(3000));
                return !producingThread.isAlive();
            }
        }));

        final DestinationViewMBean view = createView(destination);
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                LOG.info("enqueue=" + view.getEnqueueCount() + ", dequeue=" + view.getDequeueCount()
                        + ", inflight=" + view.getInFlightCount() + ", expired= " + view.getExpiredCount()
                        + ", size= " + view.getQueueSize());
                return sendCount == view.getExpiredCount();
            }
        }, Wait.MAX_WAIT_MILLIS * 5);
        LOG.info("enqueue=" + view.getEnqueueCount() + ", dequeue=" + view.getDequeueCount()
                + ", inflight=" + view.getInFlightCount() + ", expired= " + view.getExpiredCount()
                + ", size= " + view.getQueueSize());

        assertEquals("Not all sent messages have expired", sendCount, view.getExpiredCount());
        assertEquals("memory usage doesn't go to duck egg", 0, view.getMemoryPercentUsage());
    }

    // first ack delivered after expiry
    public void testExpiredMessagesWithVerySlowConsumer() throws Exception {
        createBroker();
        final long queuePrefetch = 600;
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                connectionUri + "?jms.prefetchPolicy.queuePrefetch=" + queuePrefetch);
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
                    waitCondition.await(6, TimeUnit.MINUTES);
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

        assertTrue("producer failed to complete within allocated time", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                producingThread.join(1000);
                return !producingThread.isAlive();
            }
        }, Wait.MAX_WAIT_MILLIS * 10));

        final DestinationViewMBean view = createView(destination);

        assertTrue("all dispatched up to default prefetch ", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return queuePrefetch == view.getDispatchCount();
            }
        }));
        assertTrue("Not all sent have expired ", Wait.waitFor(new Wait.Condition() {
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
                // consumer ackLater(delivery ack for expired messages) is based on half the prefetch value
                // which will leave half of the prefetch pending till consumer close
                return (queuePrefetch/2) -1 == view.getInFlightCount();
            }
        });
        LOG.info("enqueue=" + view.getEnqueueCount() + ", dequeue=" + view.getDequeueCount()
                + ", inflight=" + view.getInFlightCount() + ", expired= " + view.getExpiredCount()
                + ", size= " + view.getQueueSize());

        assertEquals("inflight didn't reduce to half prefetch minus single delivered message",
                     (queuePrefetch/2) -1, view.getInFlightCount());
        assertEquals("size didn't get back to 0 ", 0, view.getQueueSize());
        assertEquals("dequeues didn't match sent/expired ", sendCount, view.getDequeueCount());

        consumer.close();

        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return 0 == view.getInFlightCount();
            }
        });
        assertEquals("inflight goes to zeor on close", 0, view.getInFlightCount());

        LOG.info("done: " + getName());
    }

    public void testExpiredMessagesWithVerySlowConsumerCanContinue() throws Exception {
        createBroker();
        final long queuePrefetch = 600;
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                connectionUri + "?jms.prefetchPolicy.queuePrefetch=" + queuePrefetch);
        connection = factory.createConnection();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        producer = session.createProducer(destination);
        final int ttl = 4000;
        producer.setTimeToLive(ttl);

        final long sendCount = 1500;
        final CountDownLatch receivedOneCondition = new CountDownLatch(1);
        final CountDownLatch waitCondition = new CountDownLatch(1);
        final AtomicLong received = new AtomicLong();
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new MessageListener() {

            public void onMessage(Message message) {
                try {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Got my message: " + message);
                    }
                    receivedOneCondition.countDown();
                    received.incrementAndGet();
                    waitCondition.await(5, TimeUnit.MINUTES);
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("acking message: " + message);
                    }
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

        assertTrue("producer failed to complete within allocated time", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                producingThread.join(1000);
                return !producingThread.isAlive();
            }
        }, Wait.MAX_WAIT_MILLIS * 10));

        final DestinationViewMBean view = createView(destination);

        assertTrue("Not all dispatched up to default prefetch ", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return queuePrefetch == view.getDispatchCount();
            }
        }));
        assertTrue("All have not sent have expired ", Wait.waitFor(new Wait.Condition() {
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
                // consumer ackLater(delivery ack for expired messages) is based on half the prefetch value
                // which will leave half of the prefetch pending till consumer close
                return (queuePrefetch/2) -1 == view.getInFlightCount();
            }
        });
        LOG.info("enqueue=" + view.getEnqueueCount() + ", dequeue=" + view.getDequeueCount()
                + ", inflight=" + view.getInFlightCount() + ", expired= " + view.getExpiredCount()
                + ", size= " + view.getQueueSize());

        assertEquals("inflight didn't reduce to half prefetch minus single delivered message",
                     (queuePrefetch/2) -1, view.getInFlightCount());
        assertEquals("size doesn't get back to 0 ", 0, view.getQueueSize());
        assertEquals("dequeues don't match sent/expired ", sendCount, view.getDequeueCount());

        // produce some more
        producer.setTimeToLive(0);
        long tStamp = System.currentTimeMillis();
        for (int i=0; i<sendCount; i++) {
            producer.send(session.createTextMessage("test-" + i));
            if (i%100 == 0) {
                LOG.info("sent: " + i + " @ " + ((System.currentTimeMillis() - tStamp) / 100)  + "m/ms");
                tStamp = System.currentTimeMillis() ;
            }
        }

        Wait.waitFor(new Wait.Condition() {
             public boolean isSatisified() throws Exception {
                 return received.get() >= sendCount;
             }
         });

        consumer.close();

        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return 0 == view.getInFlightCount();
            }
        });
        assertEquals("inflight did not go to zeor on close", 0, view.getInFlightCount());

        LOG.info("done: " + getName());
    }

    public void testExpireMessagesForDurableSubscriber() throws Exception {
        createBroker();
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        connection = factory.createConnection();
        connection.setClientID("myConnection");
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        connection.start();
        Topic destination = session.createTopic("test");
        producer = session.createProducer(destination);
        final int ttl = 300;
        producer.setTimeToLive(ttl);

        final long sendCount = 10;

        TopicSubscriber sub = session.createDurableSubscriber(destination, "mySub");
        sub.close();

        for (int i=0; i < sendCount; i++) {
            producer.send(session.createTextMessage("test"));
        }

        DestinationViewMBean view = createView((ActiveMQTopic)destination);

        LOG.info("messages sent");
        LOG.info("expired=" + view.getExpiredCount() + " " +  view.getEnqueueCount());
        assertEquals(0, view.getExpiredCount());
        assertEquals(10, view.getEnqueueCount());

        Thread.sleep(4000);

        LOG.info("expired=" + view.getExpiredCount() + " " +  view.getEnqueueCount());
        assertEquals(10, view.getExpiredCount());
        assertEquals(0, view.getEnqueueCount());

        final AtomicLong received = new AtomicLong();
        sub = session.createDurableSubscriber(destination, "mySub");
        sub.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                received.incrementAndGet();
            }
        });

        LOG.info("Waiting for messages to arrive");

        Wait.waitFor(new Wait.Condition() {
             public boolean isSatisified() throws Exception {
                 return received.get() >= sendCount;
             }
         }, 1000);

        LOG.info("received=" + received.get());
        LOG.info("expired=" + view.getExpiredCount() + " " +  view.getEnqueueCount());

        assertEquals(0, received.get());
        assertEquals(10, view.getExpiredCount());
        assertEquals(0, view.getEnqueueCount());
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

    public boolean getOptimizedDispatch() {
        return this.optimizedDispatch;
    }

    public void setOptimizedDispatch(boolean option) {
        this.optimizedDispatch = option;
    }

    public PendingQueueMessageStoragePolicy getPendingQueuePolicy() {
        return this.pendingQueuePolicy;
    }

    public void setPendingQueuePolicy(PendingQueueMessageStoragePolicy policy) {
        this.pendingQueuePolicy = policy;
    }
}
