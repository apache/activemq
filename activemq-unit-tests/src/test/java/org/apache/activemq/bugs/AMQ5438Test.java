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

import static org.junit.Assert.*;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.journal.DataFile;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.util.IOExceptionHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotNull;

public class AMQ5438Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ5438Test.class);

    private final String xbean = "xbean:";
    private final String confBase = "src/test/resources/org/apache/activemq/bugs/amq5438";

    private static BrokerService brokerService;
    private String connectionUri;

    private AtomicInteger ioExceptionsThrown;
    private CountDownLatch journalFileRemovedLatch;

    @Before
    public void setUp() throws Exception {
        brokerService = BrokerFactory.createBroker(xbean + confBase + "/activemq.xml");
        connectionUri = brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString();
        brokerService.setDeleteAllMessagesOnStartup(true);

        ioExceptionsThrown = new AtomicInteger(0);
        brokerService.setIoExceptionHandler(new IOExceptionHandler() {
            @Override
            public void handle(IOException exception) {
                ioExceptionsThrown.incrementAndGet();
            }

            @Override
            public void setBrokerService(BrokerService brokerService) {
                // do nothing
            }
        });

        journalFileRemovedLatch = new CountDownLatch(1);
        MultiKahaDBPersistenceAdapter mkahadb = (MultiKahaDBPersistenceAdapter)
                brokerService.getPersistenceAdapter();
        List<PersistenceAdapter> adapters = mkahadb.getAdapters();
        KahaDBPersistenceAdapter adapter = (KahaDBPersistenceAdapter) adapters.get(0);
        adapter.getStore().getJournal().setDataFileRemovedListener(new Journal.DataFileRemovedListener() {
            @Override
            public void fileRemoved(DataFile datafile) {
                journalFileRemovedLatch.countDown();
            }
        });
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test
    public void testArchiveDataLogsFailure() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        Connection connection = factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("Test");

        MessageConsumer consumer = session.createConsumer(queue);
        final int messagesToSend = 1000;
        final CountDownLatch latch = new CountDownLatch(messagesToSend);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                latch.countDown();
            }
        });

        final int payloadSize = 1024;
        byte[] payload = createTestPayload(payloadSize);

        LOG.info("Sending test messages");
        MessageProducer producer = session.createProducer(queue);
        for (int i = 0; i < messagesToSend; i++) {
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes(payload);
            producer.send(bytesMessage);
        }


        LOG.info("Sent. Awaiting receipt.");
        latch.await();
        LOG.info("Test messages received.");

        final int cleanupInterval = 5000 * 2; // double the setting in activemq.xml; no listener to latch onto
        LOG.info("Checking whether journal files archived");
        assertTrue("Journal file not removed during wait time",
                journalFileRemovedLatch.await(cleanupInterval, TimeUnit.MILLISECONDS));
        assertEquals("Broker service encountered IOException moving archive file", 0, ioExceptionsThrown.get());

        consumer.close();
        producer.close();
        session.close();
        connection.close();
    }

    private byte[] createTestPayload(int payloadSize) {
        final byte[] payload = new byte[payloadSize];
        for (int i = 0; i < payloadSize; i++) {
            payload[i] = 0;
        }
        return payload;
    }

    private BrokerViewMBean getBrokerView(String testDurable) throws MalformedObjectNameException {
        ObjectName brokerName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost");
        BrokerViewMBean view = (BrokerViewMBean) brokerService.getManagementContext().newProxyInstance(brokerName, BrokerViewMBean.class, true);
        assertNotNull(view);
        return view;
    }
}
