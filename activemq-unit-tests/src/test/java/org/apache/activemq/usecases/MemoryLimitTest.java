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

import java.util.Arrays;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.ConsumerThread;
import org.apache.activemq.util.ProducerThread;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(value = Parameterized.class)
public class MemoryLimitTest extends TestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryLimitTest.class);
    final byte[] payload = new byte[10 * 1024]; //10KB
    protected BrokerService broker;

    @Parameterized.Parameter
    public TestSupport.PersistenceAdapterChoice persistenceAdapterChoice;

    @Parameterized.Parameters(name="store={0}")
    public static Iterable<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{{TestSupport.PersistenceAdapterChoice.KahaDB}, {PersistenceAdapterChoice.LevelDB}, {PersistenceAdapterChoice.JDBC}});
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.getSystemUsage().getMemoryUsage().setLimit(1 * 1024 * 1024); //1MB
        broker.deleteAllMessages();

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setExpireMessagesPeriod(0); // when this fires it will consume 2*pageSize mem which will throw the test
        policyEntry.setProducerFlowControl(false);
        policyMap.put(new ActiveMQQueue(">"), policyEntry);
        broker.setDestinationPolicy(policyMap);

        LOG.info("Starting broker with persistenceAdapterChoice " + persistenceAdapterChoice.toString());
        setPersistenceAdapter(broker, persistenceAdapterChoice);
        broker.getPersistenceAdapter().deleteAllMessages();

        return broker;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        broker.start();
        broker.waitUntilStarted();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 640000)
    public void testCursorBatch() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?jms.prefetchPolicy.all=10");
        factory.setOptimizeAcknowledge(true);
        Connection conn = factory.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = sess.createQueue("STORE");
        final ProducerThread producer = new ProducerThread(sess, queue) {
            @Override
            protected Message createMessage(int i) throws Exception {
                BytesMessage bytesMessage = session.createBytesMessage();
                bytesMessage.writeBytes(payload);
                return bytesMessage;
            }
        };
        producer.setMessageCount(2000);
        producer.start();
        producer.join();

        Thread.sleep(1000);

        // assert we didn't break high watermark (70%) usage
        final Destination dest = broker.getDestination((ActiveMQQueue) queue);
        LOG.info("Destination usage: " + dest.getMemoryUsage());
        int percentUsage = dest.getMemoryUsage().getPercentUsage();
        assertTrue("Should be less than 70% of limit but was: " + percentUsage, percentUsage <= 71);
        LOG.info("Broker usage: " + broker.getSystemUsage().getMemoryUsage());
        assertTrue(broker.getSystemUsage().getMemoryUsage().getPercentUsage() <= 71);

        // consume one message
        MessageConsumer consumer = sess.createConsumer(queue);
        Message msg = consumer.receive(5000);
        msg.acknowledge();

        // this should free some space and allow us to get new batch of messages in the memory
        // exceeding the limit
        assertTrue("Limit is exceeded", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Destination usage: " + dest.getMemoryUsage());
                return dest.getMemoryUsage().getPercentUsage() >= 200;
            }
        }));

        LOG.info("Broker usage: " + broker.getSystemUsage().getMemoryUsage());
        assertTrue(broker.getSystemUsage().getMemoryUsage().getPercentUsage() >= 200);

        // let's make sure we can consume all messages
        for (int i = 1; i < 2000; i++) {
            msg = consumer.receive(5000);
            if (msg == null) {
                dumpAllThreads("NoMessage");
            }
            assertNotNull("Didn't receive message " + i, msg);
            msg.acknowledge();
        }
    }

    @Test(timeout = 120000)
    public void testMoveMessages() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?jms.prefetchPolicy.all=10");
        factory.setOptimizeAcknowledge(true);
        Connection conn = factory.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = sess.createQueue("IN");
        final byte[] payload = new byte[200 * 1024]; //200KB
        final int count = 4;
        final ProducerThread producer = new ProducerThread(sess, queue) {
            @Override
            protected Message createMessage(int i) throws Exception {
                BytesMessage bytesMessage = session.createBytesMessage();
                bytesMessage.writeBytes(payload);
                return bytesMessage;
            }
        };
        producer.setMessageCount(count);
        producer.start();
        producer.join();

        Thread.sleep(1000);

        final QueueViewMBean in = getProxyToQueue("IN");
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return in.getQueueSize() == count;
            }
        });

        assertEquals("Messages not sent" , count, in.getQueueSize());

        int moved = in.moveMatchingMessagesTo("", "OUT");

        assertEquals("Didn't move all messages", count, moved);


        final QueueViewMBean out = getProxyToQueue("OUT");

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return out.getQueueSize() == count;
            }
        });

        assertEquals("Messages not moved" , count, out.getQueueSize());

    }

    /**
     * Handy test for manually checking what's going on
     */
    @Ignore
    @Test(timeout = 120000)
    public void testLimit() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?jms.prefetchPolicy.all=10");
        factory.setOptimizeAcknowledge(true);
        Connection conn = factory.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final ProducerThread producer = new ProducerThread(sess, sess.createQueue("STORE.1")) {
            @Override
            protected Message createMessage(int i) throws Exception {
                return session.createTextMessage(payload + "::" + i);
            }
        };
        producer.setMessageCount(1000);

        final ProducerThread producer2 = new ProducerThread(sess, sess.createQueue("STORE.2")) {
            @Override
            protected Message createMessage(int i) throws Exception {
                return session.createTextMessage(payload + "::" + i);
            }
        };
        producer2.setMessageCount(1000);

        ConsumerThread consumer = new ConsumerThread(sess, sess.createQueue("STORE.1"));
        consumer.setBreakOnNull(false);
        consumer.setMessageCount(1000);

        producer.start();
        producer.join();

        producer2.start();

        Thread.sleep(300);

        consumer.start();

        consumer.join();
        producer2.join();

        assertEquals("consumer got all produced messages", producer.getMessageCount(), consumer.getReceived());
    }

}
