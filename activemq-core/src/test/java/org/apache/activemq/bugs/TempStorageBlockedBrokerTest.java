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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.plist.PListStoreImpl;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.StoreUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.TempUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TempStorageBlockedBrokerTest extends TestSupport {

    public int deliveryMode = DeliveryMode.PERSISTENT;

    private static final Logger LOG = LoggerFactory.getLogger(TempStorageBlockedBrokerTest.class);
    private static final int MESSAGES_COUNT = 1000;
    private static byte[] buf = new byte[4 * 1024];
    private BrokerService broker;
    AtomicInteger messagesSent = new AtomicInteger(0);
    AtomicInteger messagesConsumed = new AtomicInteger(0);

    protected long messageReceiveTimeout = 10000L;

    Destination destination = new ActiveMQTopic("FooTwo");

    private String connectionUri;

    public void testRunProducerWithHungConsumer() throws Exception {

        final long origTempUsage = broker.getSystemUsage().getTempUsage().getUsage();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        // ensure messages are spooled to disk for this consumer
        ActiveMQPrefetchPolicy prefetch = new ActiveMQPrefetchPolicy();
        prefetch.setTopicPrefetch(10);
        factory.setPrefetchPolicy(prefetch);
        Connection consumerConnection = factory.createConnection();
        consumerConnection.start();

        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(destination);

        final Connection producerConnection = factory.createConnection();
        producerConnection.start();

        final CountDownLatch producerHasSentTenMessages = new CountDownLatch(10);
        Thread producingThread = new Thread("Producing thread") {
            @Override
            public void run() {
                try {
                    Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(destination);
                    producer.setDeliveryMode(deliveryMode);
                    for (int idx = 0; idx < MESSAGES_COUNT; ++idx) {
                        Message message = session.createTextMessage(new String(buf) + idx);

                        producer.send(message);
                        messagesSent.incrementAndGet();
                        producerHasSentTenMessages.countDown();
                        Thread.sleep(10);
                        if (idx != 0 && idx%100 == 0) {
                            LOG.info("Sent Message " + idx);
                            LOG.info("Temp Store Usage " + broker.getSystemUsage().getTempUsage().getUsage());
                        }
                    }
                    producer.close();
                    session.close();
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };
        producingThread.start();

        assertTrue("producer has sent 10 in a reasonable time", producerHasSentTenMessages.await(30, TimeUnit.SECONDS));

        int count = 0;

        Message m = null;
        while ((m = consumer.receive(messageReceiveTimeout)) != null) {
            count++;
            if (count != 0 && count%10 == 0) {
                LOG.info("Recieved Message (" + count + "):" + m);
            }
            messagesConsumed.incrementAndGet();
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                LOG.info("error sleeping");
            }
        }

        LOG.info("Connection Timeout: Retrying.. count: " + count);

        while ((m = consumer.receive(messageReceiveTimeout)) != null) {
            count++;
            if (count != 0 && count%100 == 0) {
                LOG.info("Recieved Message (" + count + "):" + m);
            }
            messagesConsumed.incrementAndGet();
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                LOG.info("error sleeping");
            }
        }

        LOG.info("consumer session closing: consumed count: " + count);

        consumerSession.close();

        producingThread.join();

        final long tempUsageBySubscription = broker.getSystemUsage().getTempUsage().getUsage();
        LOG.info("Orig Usage: " + origTempUsage + ", currentUsage: " + tempUsageBySubscription);

        producerConnection.close();
        consumerConnection.close();

        LOG.info("Subscrition Usage: " + tempUsageBySubscription + ", endUsage: "
                + broker.getSystemUsage().getTempUsage().getUsage());

        // do a cleanup
        ((PListStoreImpl)broker.getTempDataStore()).run();
        LOG.info("Subscrition Usage: " + tempUsageBySubscription + ", endUsage: "
                        + broker.getSystemUsage().getTempUsage().getUsage());

        assertEquals("Incorrect number of Messages Sent: " + messagesSent.get(), messagesSent.get(), MESSAGES_COUNT);
        assertEquals("Incorrect number of Messages Consumed: " + messagesConsumed.get(), messagesConsumed.get(),
                MESSAGES_COUNT);
    }

    public void testFillTempAndConsume() throws Exception {

        broker.getSystemUsage().setSendFailIfNoSpace(true);
        destination = new ActiveMQQueue("Foo");

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        final ActiveMQConnection producerConnection = (ActiveMQConnection) factory.createConnection();
        // so we can easily catch the ResourceAllocationException on send
        producerConnection.setAlwaysSyncSend(true);
        producerConnection.start();

        Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        try {
            while (true) {
                Message message = session.createTextMessage(new String(buf) + messagesSent.toString());
                producer.send(message);
                messagesSent.incrementAndGet();
                if (messagesSent.get() % 100 == 0) {
                    LOG.info("Sent Message " + messagesSent.get());
                    LOG.info("Temp Store Usage " + broker.getSystemUsage().getTempUsage().getUsage());
                }
            }
        } catch (ResourceAllocationException ex) {
            LOG.info("Got resource exception : " + ex + ", after sent: " + messagesSent.get());
        }

        // consume all sent
        Connection consumerConnection = factory.createConnection();
        consumerConnection.start();

        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(destination);


        while (consumer.receive(messageReceiveTimeout) != null) {
            messagesConsumed.incrementAndGet();
            if (messagesConsumed.get() % 1000 == 0) {
                LOG.info("received Message " + messagesConsumed.get());
                LOG.info("Temp Store Usage " + broker.getSystemUsage().getTempUsage().getUsage());
            }
        }

        assertEquals("Incorrect number of Messages Consumed: " + messagesConsumed.get(), messagesConsumed.get(),
                messagesSent.get());
    }

    @Override
    public void setUp() throws Exception {

        broker = new BrokerService();
        broker.setDataDirectory("target" + File.separator + "activemq-data");
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setAdvisorySupport(false);
        broker.setDeleteAllMessagesOnStartup(true);

        setDefaultPersistenceAdapter(broker);
        SystemUsage sysUsage = broker.getSystemUsage();
        MemoryUsage memUsage = new MemoryUsage();
        memUsage.setLimit((1024 * 1024));
        StoreUsage storeUsage = new StoreUsage();
        storeUsage.setLimit((1024 * 1024) * 38);
        TempUsage tmpUsage = new TempUsage();
        tmpUsage.setLimit((1024 * 1024) * 38);

        PolicyEntry defaultPolicy = new PolicyEntry();
        // defaultPolicy.setTopic("FooTwo");
        defaultPolicy.setProducerFlowControl(false);
        defaultPolicy.setMemoryLimit(10 * 1024);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(defaultPolicy);

        sysUsage.setMemoryUsage(memUsage);
        sysUsage.setStoreUsage(storeUsage);
        sysUsage.setTempUsage(tmpUsage);

        broker.setDestinationPolicy(policyMap);
        broker.setSystemUsage(sysUsage);

        broker.addConnector("tcp://localhost:0").setName("Default");
        broker.start();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @Override
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

}
