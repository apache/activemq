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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ2512Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ2512Test.class);

    private final String QUEUE_NAME = "dee.q";
    private final int INITIAL_MESSAGES_CNT = 1000;
    private final int WORKER_INTERNAL_ITERATIONS = 100;
    private final int TOTAL_MESSAGES_CNT = INITIAL_MESSAGES_CNT * WORKER_INTERNAL_ITERATIONS + INITIAL_MESSAGES_CNT;
    private final byte[] payload = new byte[5 * 1024];
    private final String TEXT = new String(payload);

    private final String PRP_INITIAL_ID = "initial-id";
    private final String PRP_WORKER_ID = "worker-id";

    private final CountDownLatch LATCH = new CountDownLatch(TOTAL_MESSAGES_CNT);
    private final AtomicInteger ON_MSG_COUNTER = new AtomicInteger();

    private BrokerService brokerService;
    private Connection connection;
    private String connectionURI;

    @Test(timeout = 5*60000)
    public void testKahaDBFailure() throws Exception {
        final ConnectionFactory fac = new ActiveMQConnectionFactory(connectionURI);
        connection = fac.createConnection();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue queue = session.createQueue(QUEUE_NAME);
        final MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        connection.start();

        final long startTime = System.nanoTime();

        final List<Consumer> consumers = new ArrayList<Consumer>();
        for (int i = 0; i < 20; i++) {
            consumers.add(new Consumer("worker-" + i));
        }

        for (int i = 0; i < INITIAL_MESSAGES_CNT; i++) {
            final TextMessage msg = session.createTextMessage(TEXT);
            msg.setStringProperty(PRP_INITIAL_ID, "initial-" + i);
            producer.send(msg);
        }

        LATCH.await();
        final long endTime = System.nanoTime();
        LOG.info("Total execution time = "
                + TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS) + " [ms].");
        LOG.info("Rate = " + TOTAL_MESSAGES_CNT
                / TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS) + " [msg/s].");

        for (Consumer c : consumers) {
            c.close();
        }

        connection.close();
    }

    private final class Consumer implements MessageListener {

        private final String name;
        private final Session session;
        private final MessageProducer producer;

        private Consumer(String name) {
            this.name = name;
            try {
                session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                final Queue queue = session.createQueue(QUEUE_NAME + "?consumer.prefetchSize=10");
                producer = session.createProducer(queue);
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                final MessageConsumer consumer = session.createConsumer(queue);
                consumer.setMessageListener(this);
            } catch (JMSException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onMessage(Message message) {
            final TextMessage msg = (TextMessage) message;
            try {
                if (!msg.propertyExists(PRP_WORKER_ID)) {
                    for (int i = 0; i < WORKER_INTERNAL_ITERATIONS; i++) {
                        final TextMessage newMsg = session.createTextMessage(msg.getText());
                        newMsg.setStringProperty(PRP_WORKER_ID, name + "-" + i);
                        newMsg.setStringProperty(PRP_INITIAL_ID, msg.getStringProperty(PRP_INITIAL_ID));
                        producer.send(newMsg);
                    }
                }
                msg.acknowledge();

            } catch (JMSException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                final int onMsgCounter = ON_MSG_COUNTER.getAndIncrement();
                if (onMsgCounter % 1000 == 0) {
                    LOG.info("message received: " + onMsgCounter);
                }
                LATCH.countDown();
            }
        }

        private void close() {
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.start();

        connectionURI = brokerService.getTransportConnectorByName("openwire").getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    protected BrokerService createBroker() throws Exception {
        File dataFileDir = new File("target/test-amq-2512/datadb");
        IOHelper.mkdirs(dataFileDir);
        IOHelper.deleteChildren(dataFileDir);

        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(dataFileDir);
        kaha.setEnableJournalDiskSyncs(false);

        BrokerService answer = new BrokerService();
        answer.setPersistenceAdapter(kaha);
        answer.setDataDirectoryFile(dataFileDir);
        answer.setUseJmx(false);
        answer.addConnector("tcp://localhost:0").setName("openwire");

        return answer;
    }
}
