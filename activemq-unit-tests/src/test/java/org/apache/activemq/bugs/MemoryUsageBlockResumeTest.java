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

import java.io.File;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runner.RunWith;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;


@RunWith(BlockJUnit4ClassRunner.class)
public class MemoryUsageBlockResumeTest extends TestSupport implements Thread.UncaughtExceptionHandler {

    public int deliveryMode = DeliveryMode.PERSISTENT;

    private static final Logger LOG = LoggerFactory.getLogger(MemoryUsageBlockResumeTest.class);
    private static byte[] buf = new byte[4 * 1024];
    private static byte[] bigBuf = new byte[48 * 1024];

    private BrokerService broker;
    AtomicInteger messagesSent = new AtomicInteger(0);
    AtomicInteger messagesConsumed = new AtomicInteger(0);

    protected long messageReceiveTimeout = 10000L;

    Destination destination = new ActiveMQQueue("FooTwo");
    Destination bigDestination = new ActiveMQQueue("FooTwoBig");

    private String connectionUri;
    private final Vector<Throwable> exceptions = new Vector<Throwable>();

    @Test(timeout = 60 * 1000)
    public void testBlockByOtherResumeNoException() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);

        // ensure more than on message can be pending when full
        factory.setProducerWindowSize(48*1024);
        // ensure messages are spooled to disk for this consumer
        ActiveMQPrefetchPolicy prefetch = new ActiveMQPrefetchPolicy();
        prefetch.setTopicPrefetch(10);
        factory.setPrefetchPolicy(prefetch);
        Connection consumerConnection = factory.createConnection();
        consumerConnection.start();

        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(bigDestination);

        final Connection producerConnection = factory.createConnection();
        producerConnection.start();

        final int fillWithBigCount = 10;
        Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);
        producer.setDeliveryMode(deliveryMode);
        for (int idx = 0; idx < fillWithBigCount; ++idx) {
            Message message = session.createTextMessage(new String(bigBuf) + idx);
            producer.send(bigDestination, message);
            messagesSent.incrementAndGet();
            LOG.info("After big: " + idx + ", System Memory Usage " + broker.getSystemUsage().getMemoryUsage().getPercentUsage());
        }

        // will block on pfc
        final int toSend = 20;
        Thread producingThread = new Thread("Producing thread") {
            @Override
            public void run() {
                try {
                    Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(destination);
                    producer.setDeliveryMode(deliveryMode);
                    for (int idx = 0; idx < toSend; ++idx) {
                        Message message = session.createTextMessage(new String(buf) + idx);
                        producer.send(destination, message);
                        messagesSent.incrementAndGet();
                        LOG.info("After little:" + idx + ", System Memory Usage " + broker.getSystemUsage().getMemoryUsage().getPercentUsage());
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };
        producingThread.start();

        Thread producingThreadTwo = new Thread("Producing thread") {
            @Override
            public void run() {
                try {
                    Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(destination);
                    producer.setDeliveryMode(deliveryMode);
                    for (int idx = 0; idx < toSend; ++idx) {
                        Message message = session.createTextMessage(new String(buf) + idx);
                        producer.send(destination, message);
                        messagesSent.incrementAndGet();
                        LOG.info("After little:" + idx + ", System Memory Usage " + broker.getSystemUsage().getMemoryUsage().getPercentUsage());
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };
        producingThreadTwo.start();

        assertTrue("producer has sent x in a reasonable time", Wait.waitFor(new Wait.Condition()
        {
            @Override
            public boolean isSatisified() throws Exception {
                 LOG.info("Checking for : X sent, System Memory Usage " + broker.getSystemUsage().getMemoryUsage().getPercentUsage() + ", sent:  " + messagesSent);
                return messagesSent.get() > 20;
            }
        }));


        LOG.info("Consuming from big q to allow delivery to smaller q from pending");
        int count = 0;

        Message m = null;

        for (;count < 10; count++) {
            assertTrue((m = consumer.receive(messageReceiveTimeout)) != null);
            LOG.info("Recieved Message (" + count + "):" + m + ", System Memory Usage " + broker.getSystemUsage().getMemoryUsage().getPercentUsage());
            messagesConsumed.incrementAndGet();
        }
        consumer.close();

        producingThread.join();
        producingThreadTwo.join();

        assertEquals("Incorrect number of Messages Sent: " + messagesSent.get(), messagesSent.get(), fillWithBigCount +  toSend*2);

        // consume all little messages
        consumer = consumerSession.createConsumer(destination);
        for (count = 0;count < toSend*2; count++) {
            assertTrue((m = consumer.receive(messageReceiveTimeout)) != null);
            LOG.info("Recieved Message (" + count + "):" + m + ", System Memory Usage " + broker.getSystemUsage().getMemoryUsage().getPercentUsage() );
            messagesConsumed.incrementAndGet();
        }

        assertEquals("Incorrect number of Messages consumed: " + messagesConsumed.get(), messagesSent.get(), messagesConsumed.get());

        //assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
    }

    @Override
    @Before
    public void setUp() throws Exception {

        Thread.setDefaultUncaughtExceptionHandler(this);
        broker = new BrokerService();
        broker.setDataDirectory("target" + File.separator + "activemq-data");
        broker.setPersistent(true);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setDeleteAllMessagesOnStartup(true);

        setDefaultPersistenceAdapter(broker);
        broker.getSystemUsage().getMemoryUsage().setLimit((30 * 16 * 1024));

        PolicyEntry defaultPolicy = new PolicyEntry();
        defaultPolicy.setOptimizedDispatch(true);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(defaultPolicy);
        broker.setDestinationPolicy(policyMap);

        broker.addConnector("tcp://localhost:0");
        broker.start();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        LOG.error("Unexpected Unhandeled ex on: " + t, e);
        exceptions.add(e);
    }
}
