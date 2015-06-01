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
package org.apache.activemq.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

/**
 * This test shows Inflight Message sizes are correct for various acknowledgement modes.
 */
public abstract class AbstractInflightMessageSizeTest {

    protected BrokerService brokerService;
    protected Connection connection;
    protected String brokerUrlString;
    protected Session session;
    protected javax.jms.Destination dest;
    protected Destination amqDestination;
    protected MessageConsumer consumer;
    protected int prefetch = 100;
    final protected int ackType;
    final protected boolean optimizeAcknowledge;
    final protected String destName = "testDest";

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {ActiveMQSession.SESSION_TRANSACTED, true},
                {ActiveMQSession.AUTO_ACKNOWLEDGE, true},
                {ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE, true},
                {ActiveMQSession.CLIENT_ACKNOWLEDGE, true},
                {ActiveMQSession.SESSION_TRANSACTED, false},
                {ActiveMQSession.AUTO_ACKNOWLEDGE, false},
                {ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE, false},
                {ActiveMQSession.CLIENT_ACKNOWLEDGE, false}
        });
    }

    public AbstractInflightMessageSizeTest(int ackType, boolean optimizeAcknowledge) {
        this.ackType = ackType;
        this.optimizeAcknowledge = optimizeAcknowledge;
    }

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        TransportConnector tcp = brokerService
                .addConnector("tcp://localhost:0");
        brokerService.start();
        //used to test optimizeAcknowledge works
        String optAckString = optimizeAcknowledge ? "?jms.optimizeAcknowledge=true&jms.optimizedAckScheduledAckInterval=2000" : "";
        brokerUrlString = tcp.getPublishableConnectString() + optAckString;
        connection = createConnectionFactory().createConnection();
        connection.setClientID("client1");
        connection.start();
        session = connection.createSession(ackType == ActiveMQSession.SESSION_TRANSACTED, ackType);
        dest = getDestination();
        consumer = getMessageConsumer();
        amqDestination = TestSupport.getDestination(brokerService, getActiveMQDestination());
    }

    protected ActiveMQConnectionFactory createConnectionFactory()
            throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrlString);
        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setTopicPrefetch(prefetch);
        prefetchPolicy.setQueuePrefetch(prefetch);
        prefetchPolicy.setOptimizeDurableTopicPrefetch(prefetch);
        factory.setPrefetchPolicy(prefetchPolicy);
        return factory;
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        brokerService.stop();
    }

    /**
     * Tests that inflight message size goes up and comes back down to 0 after
     * messages are consumed
     *
     * @throws javax.jms.JMSException
     * @throws InterruptedException
     */
    @Test(timeout=15000)
    public void testInflightMessageSize() throws Exception {
        final long size = sendMessages(10);

        assertTrue("Inflight message size should be greater than the content length sent", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getSubscription().getInFlightMessageSize() > size;
            }
        }));

        receiveMessages(10);

        assertTrue("Inflight message size should be 0", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getSubscription().getInFlightMessageSize() == 0;
            }
        }));
    }

    /**
     * Test that the in flight message size won't rise after prefetch is filled
     *
     * @throws Exception
     */
    @Test(timeout=15000)
    public void testInflightMessageSizePrefetchFilled() throws Exception {
        final long size = sendMessages(prefetch);

        assertTrue("Inflight message size should be greater than content length", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getSubscription().getInFlightMessageSize() > size;
            }
        }));

        final long inFlightSize = getSubscription().getInFlightMessageSize();
        sendMessages(10);

        //Prefetch has been filled, so the size should not change with 10 more messages
        assertEquals("Inflight message size should not change", inFlightSize, getSubscription().getInFlightMessageSize());

        receiveMessages(prefetch + 10);

        assertTrue("Inflight message size should be 0", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getSubscription().getInFlightMessageSize() == 0;
            }
        }));
    }

    /**
     * Test that the in flight message size will still rise if prefetch is not filled
     *
     * @throws Exception
     */
    @Test(timeout=15000)
    public void testInflightMessageSizePrefetchNotFilled() throws Exception {
        final long size = sendMessages(prefetch - 10);

        assertTrue("Inflight message size should be greater than content length", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getSubscription().getInFlightMessageSize() > size;
            }
        }));

        //capture the inflight size and send 10 more messages
        final long inFlightSize = getSubscription().getInFlightMessageSize();
        sendMessages(10);

        //Prefetch has NOT been filled, so the size should rise with 10 more messages
        assertTrue("Inflight message size should be greater than previous inlight size", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getSubscription().getInFlightMessageSize() > inFlightSize;
            }
        }));

        receiveMessages(prefetch);

        assertTrue("Inflight message size should be 0", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getSubscription().getInFlightMessageSize() == 0;
            }
        }));
    }


    /**
     * Tests that inflight message size goes up and doesn't go down if receive is rolledback
     *
     * @throws javax.jms.JMSException
     * @throws InterruptedException
     */
    @Test(timeout=15000)
    public void testInflightMessageSizeRollback() throws Exception {
        Assume.assumeTrue(ackType == ActiveMQSession.SESSION_TRANSACTED);

        final long size = sendMessages(10);

        assertTrue("Inflight message size should be greater than the content length sent", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return getSubscription().getInFlightMessageSize() > size;
            }
        }));

       long inFlightSize = getSubscription().getInFlightMessageSize();

        for (int i = 0; i < 10; i++) {
            consumer.receive();
        }
        session.rollback();

        assertEquals("Inflight message size should not change on rollback", inFlightSize, getSubscription().getInFlightMessageSize());
    }

    /**
     * This method will generate random sized messages up to 150000 bytes.
     *
     * @param count
     * @throws JMSException
     */
    protected long sendMessages(int count) throws JMSException {
        MessageProducer producer = session.createProducer(dest);
        long totalSize = 0;
        for (int i = 0; i < count; i++) {
            Random r = new Random();
            int size = r.nextInt(150000);
            totalSize += size;
            byte[] bytes = new byte[size > 0 ? size : 1];
            r.nextBytes(bytes);
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(bytes);
            producer.send(bytesMessage);
        }
        if (session.getTransacted()) {
            session.commit();
        }
        return totalSize;
    }

    protected void receiveMessages(int count) throws JMSException {
        for (int i = 0; i < count; i++) {
            javax.jms.Message m = consumer.receive();
            if (ackType == ActiveMQSession.SESSION_TRANSACTED) {
                session.commit();
            } else if (ackType != ActiveMQSession.AUTO_ACKNOWLEDGE) {
                m.acknowledge();
            }
        }
    }

    protected abstract Subscription getSubscription();

    protected abstract ActiveMQDestination getActiveMQDestination();

    protected abstract MessageConsumer getMessageConsumer() throws JMSException;

    protected abstract javax.jms.Destination getDestination() throws JMSException ;

}
