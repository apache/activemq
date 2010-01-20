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

import static org.junit.Assert.assertEquals;
import java.io.File;
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
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.amq.AMQPersistenceAdapter;
import org.apache.activemq.store.kahadb.plist.PListStore;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.StoreUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.TempUsage;
import org.apache.activemq.util.IOHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TempStorageBlockedBrokerTest {

    public boolean consumeAll = false;
    public int deliveryMode = DeliveryMode.PERSISTENT;

    private static final Log LOG = LogFactory.getLog(TempStorageBlockedBrokerTest.class);
    private static final int MESSAGES_COUNT = 1000;
    private static byte[] buf = new byte[4 * 1024];
    private BrokerService broker;
    AtomicInteger messagesSent = new AtomicInteger(0);
    AtomicInteger messagesConsumed = new AtomicInteger(0);

    protected long messageReceiveTimeout = 10L;

    Destination destination = new ActiveMQTopic("FooTwo");

    @Test
    public void runProducerWithHungConsumer() throws Exception {

        final long origTempUsage = broker.getSystemUsage().getTempUsage().getUsage();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61618");
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
                        Thread.sleep(10);
                        LOG.info("Sent Message " + idx);
                        LOG.info("Temp Store Usage " + broker.getSystemUsage().getTempUsage().getUsage());

                    }
                    producer.close();
                    session.close();
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };
        producingThread.start();

        int count = 0;

        Message m = null;
        while ((m = consumer.receive(messageReceiveTimeout)) != null) {
            count++;
            LOG.info("Recieved Message (" + count + "):" + m);
            messagesConsumed.incrementAndGet();
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                LOG.info("error sleeping");
            }
        }

        LOG.info("Connection Timeout: Retrying");

        // session.close();
        // consumerConnection.close();
        //
        // consumerConnection2.start();
        // session2 = consumerConnection2.createSession(false,
        // Session.AUTO_ACKNOWLEDGE);
        // consumer = session2.createConsumer(destination);

        while ((m = consumer.receive(messageReceiveTimeout)) != null) {
            count++;
            LOG.info("Recieved Message (" + count + "):" + m);
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
        // assertTrue("some temp store has been used", tempUsageBySubscription
        // != origTempUsage);

        producerConnection.close();
        consumerConnection.close();

        LOG.info("Subscrition Usage: " + tempUsageBySubscription + ", endUsage: "
                + broker.getSystemUsage().getTempUsage().getUsage());


        assertEquals("Incorrect number of Messages Sent: " + messagesSent.get(), messagesSent.get(), MESSAGES_COUNT);
        assertEquals("Incorrect number of Messages Consumed: " + messagesConsumed.get(), messagesConsumed.get(),
                MESSAGES_COUNT);
    }

    @Before
    public void setUp() throws Exception {

        broker = new BrokerService();
        broker.setDataDirectory("target" + File.separator + "activemq-data");
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setAdvisorySupport(false);
        broker.setDeleteAllMessagesOnStartup(true);

        AMQPersistenceAdapter persistence = new AMQPersistenceAdapter();
        persistence.setSyncOnWrite(false);
        File directory = new File("target" + File.separator + "activemq-data");
        persistence.setDirectory(directory);
        File tmpDir = new File(directory, "tmp");
        IOHelper.deleteChildren(tmpDir);
        PListStore tempStore = new PListStore();
        tempStore.setDirectory(tmpDir);
        tempStore.start();

        SystemUsage sysUsage = new SystemUsage("mySysUsage", persistence, tempStore);
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
        broker.setTempDataStore(tempStore);
        broker.setPersistenceAdapter(persistence);

        broker.addConnector("tcp://localhost:61618").setName("Default");
        broker.start();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

}
