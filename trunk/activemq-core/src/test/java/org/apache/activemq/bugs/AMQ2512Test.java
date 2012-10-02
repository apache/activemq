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
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.IOHelper;

public class AMQ2512Test extends EmbeddedBrokerTestSupport {
    private static Connection connection;
    private final static String QUEUE_NAME = "dee.q";
    private final static int INITIAL_MESSAGES_CNT = 1000;
    private final static int WORKER_INTERNAL_ITERATIONS = 100;
    private final static int TOTAL_MESSAGES_CNT = INITIAL_MESSAGES_CNT * WORKER_INTERNAL_ITERATIONS
            + INITIAL_MESSAGES_CNT;
    private final static byte[] payload = new byte[5 * 1024];
    private final static String TEXT = new String(payload);

    private final static String PRP_INITIAL_ID = "initial-id";
    private final static String PRP_WORKER_ID = "worker-id";

    private final static CountDownLatch LATCH = new CountDownLatch(TOTAL_MESSAGES_CNT);

    private final static AtomicInteger ON_MSG_COUNTER = new AtomicInteger();

    public void testKahaDBFailure() throws Exception {
        final ConnectionFactory fac = new ActiveMQConnectionFactory(this.bindAddress);
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
        System.out.println("Total execution time = "
                + TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS) + " [ms].");
        System.out.println("Rate = " + TOTAL_MESSAGES_CNT
                / TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS) + " [msg/s].");

        for (Consumer c : consumers) {
            c.close();
        }
        connection.close();
    }

    private final static class Consumer implements MessageListener {
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
                    System.out.println("message received: " + onMsgCounter);
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

    @Override
    protected void setUp() throws Exception {
        bindAddress = "tcp://0.0.0.0:61617";
        super.setUp();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        File dataFileDir = new File("target/test-amq-2512/datadb");
        IOHelper.mkdirs(dataFileDir);
        IOHelper.deleteChildren(dataFileDir);
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(dataFileDir); 
        BrokerService answer = new BrokerService();
        answer.setPersistenceAdapter(kaha);
      
        kaha.setEnableJournalDiskSyncs(false);
        //kaha.setIndexCacheSize(10);
        answer.setDataDirectoryFile(dataFileDir);
        answer.setUseJmx(false);
        answer.addConnector(bindAddress);
        return answer;
    }
}
