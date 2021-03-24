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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.activemq.util.TestUtils.createXid;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class MKahaDBTxVirtualTopicTest {

    static final Logger LOG = LoggerFactory.getLogger(MKahaDBTxVirtualTopicTest.class);
    private final static int maxFileLength = 1024 * 32;

    private final static int CLEANUP_INTERVAL_MILLIS = 500;

    @Parameterized.Parameter(0)
    public boolean concurrentSendOption;

    @Parameterized.Parameters(name="concurrentSend:{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {Boolean.TRUE},
                {Boolean.FALSE}
        });
    }

    BrokerService broker;

    class ConsumerHolder {

        Connection connection;
        Session session;
        MessageConsumer consumer;
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    protected BrokerService createBroker(PersistenceAdapter kaha) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setAdvisorySupport(false);
        broker.setUseJmx(true);
        broker.setBrokerName("localhost");
        broker.setPersistenceAdapter(kaha);

        VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
        VirtualTopic virtualTopic = new VirtualTopic();
        virtualTopic.setName("VirtualTopic.>");
        virtualTopic.setPrefix("Consumer.*.*.");
        virtualTopic.setConcurrentSend(concurrentSendOption);
        VirtualDestination[] virtualDestinations = {virtualTopic};
        interceptor.setVirtualDestinations(virtualDestinations);

        broker.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});

        return broker;
    }

    @Test
    public void testConcurrentSendOkWithSplitStores() throws Exception {

        prepareBrokerWithMultiStore(true);
        broker.start();
        broker.waitUntilStarted();

        // Ensure we have an Admin View.
        assertTrue("Broker doesn't have an Admin View.", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return (broker.getAdminView()) != null;
            }
        }));

        ActiveMQXAConnectionFactory activeMQXAConnectionFactory = new ActiveMQXAConnectionFactory("vm://localhost");
        init(activeMQXAConnectionFactory);
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        init(activeMQConnectionFactory);
        final Connection connection = activeMQConnectionFactory.createConnection();
        connection.start();

        String[] consumerIds = new String[]{"A.A", "A.B", "B.A", "B.B"};

        ConsumerHolder[] consumerHolders = new ConsumerHolder[consumerIds.length];

        int consumerHolderIdx = 0;
        for (final String consumerId : consumerIds) {
            consumerHolders[consumerHolderIdx++] = createConsumer(activeMQConnectionFactory, consumerId);
        }

        // send with xa
        XAConnection xaConnection = activeMQXAConnectionFactory.createXAConnection();
        xaConnection.start();
        XASession xas = xaConnection.createXASession();
        MessageProducer producer = xas.createProducer(null);

        ActiveMQTopic virtualTopic = new ActiveMQTopic("VirtualTopic.A");
        BytesMessage message = xas.createBytesMessage();

        message.writeBytes(new byte[100]);
        XAResource xaResource = xas.getXAResource();

        final int numMessages = 500;
        for (int i = 0; i < numMessages; i++) {
            message.setIntProperty("C", i);
            Xid xid = createXid();
            xaResource.start(xid, XAResource.TMNOFLAGS);

            producer.send(virtualTopic, message);

            xaResource.end(xid, XAResource.TMSUCCESS);
            xaResource.commit(xid, true);
        }

        // verify commit completed
        for (final String consumerId : consumerIds) {
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    Destination destination = broker.getDestination(new ActiveMQQueue("Consumer." + consumerId + ".VirtualTopic.A"));
                    LOG.info("message count for: " + consumerId + ", " + destination.getMessageStore().getMessageCount());
                    return numMessages == destination.getMessageStore().getMessageCount();
                }
            }));
        }


        for (int i = 0; i < numMessages; i++) {
            for (ConsumerHolder consumerHolder : consumerHolders) {
                Message m = consumerHolder.consumer.receive(4000);
                if (m != null && i == 50) {
                    LOG.info("@ 50 Got: " + m.getIntProperty("C"));
                }
                if (consumerHolder.session.getTransacted()) {
                    consumerHolder.session.commit();
                }
            }
        }

        // verify consumption
        for (final String consumerId : consumerIds) {
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    Destination destination = broker.getDestination(new ActiveMQQueue("Consumer." + consumerId + ".VirtualTopic.A"));
                    return 0 == destination.getMessageStore().getMessageCount();
                }
            }));
        }
    }

    private void init(ActiveMQConnectionFactory f) {
        f.setWatchTopicAdvisories(false);
        f.setAlwaysSyncSend(true);
    }

    protected KahaDBPersistenceAdapter createStore(boolean delete) throws IOException {
        KahaDBPersistenceAdapter kaha = new KahaDBPersistenceAdapter();
        kaha.setJournalMaxFileLength(maxFileLength);
        kaha.setCleanupInterval(CLEANUP_INTERVAL_MILLIS);
        if (delete) {
            kaha.deleteAllMessages();
        }
        return kaha;
    }

    public void prepareBrokerWithMultiStore(boolean deleteAllMessages) throws Exception {

        MultiKahaDBPersistenceAdapter multiKahaDBPersistenceAdapter = new MultiKahaDBPersistenceAdapter();
        if (deleteAllMessages) {
            multiKahaDBPersistenceAdapter.deleteAllMessages();
        }
        ArrayList<FilteredKahaDBPersistenceAdapter> adapters = new ArrayList<FilteredKahaDBPersistenceAdapter>();

        adapters.add(createFilteredKahaDBByDestinationPrefix("Consumer.A", deleteAllMessages));
        adapters.add(createFilteredKahaDBByDestinationPrefix("Consumer.B", deleteAllMessages));
        adapters.add(createFilteredKahaDBByDestinationPrefix(null, deleteAllMessages)); // for the virtual topic!

        multiKahaDBPersistenceAdapter.setFilteredPersistenceAdapters(adapters);
        multiKahaDBPersistenceAdapter.setJournalMaxFileLength(4 * 1024);
        multiKahaDBPersistenceAdapter.setJournalCleanupInterval(10);

        broker = createBroker(multiKahaDBPersistenceAdapter);
    }

    private FilteredKahaDBPersistenceAdapter createFilteredKahaDBByDestinationPrefix(String destinationPrefix,
                                                                                     boolean deleteAllMessages) throws IOException {
        FilteredKahaDBPersistenceAdapter template = new FilteredKahaDBPersistenceAdapter();
        template.setPersistenceAdapter(createStore(deleteAllMessages));
        if (destinationPrefix != null) {
            template.setQueue(destinationPrefix + ".>");
        }
        return template;
    }

    private ConsumerHolder createConsumer(ActiveMQConnectionFactory f, String id) throws JMSException {
        ConsumerHolder consumerHolder = new ConsumerHolder();
        consumerHolder.connection = f.createConnection();
        consumerHolder.connection.start();
        consumerHolder.session = consumerHolder.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumerHolder.consumer = consumerHolder.session.createConsumer(new ActiveMQQueue("Consumer." + id + ".VirtualTopic.A"));

        return consumerHolder;
    }
}