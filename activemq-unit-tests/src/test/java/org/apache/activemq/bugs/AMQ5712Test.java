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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.store.kahadb.plist.PListStoreImpl;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test behavior of senders when broker side producer flow control kicks in.
 */
public class AMQ5712Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ5712Test.class);

    @Rule public TestName name = new TestName();

    private BrokerService brokerService;
    private Connection connection;

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {}
        }

        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
    }

    private Connection createConnection() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        factory.setAlwaysSyncSend(true);
        return factory.createConnection();
    }

    @Test(timeout = 120000)
    public void test() throws Exception {
        connection = createConnection();
        connection.start();

        final int MSG_COUNT = 100;

        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final Queue queue = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        final QueueViewMBean queueView = getProxyToQueue(name.getMethodName());

        byte[] payload = new byte[65535];
        Arrays.fill(payload, (byte) 255);
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicInteger counter = new AtomicInteger();

        Thread purge = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    while (!done.await(5, TimeUnit.SECONDS)) {
                        if (queueView.getBlockedSends() > 0 && queueView.getQueueSize() > 0) {
                            long queueSize = queueView.getQueueSize();
                            LOG.info("Queue send blocked at {} messages", queueSize);
                            MessageConsumer consumer = session.createConsumer(queue);
                            for (int i = 0; i < queueSize; i++) {
                                Message message = consumer.receive(60000);
                                if (message != null) {
                                    counter.incrementAndGet();
                                    message.acknowledge();
                                } else {
                                    LOG.warn("Got null message when none as expected.");
                                }
                            }
                            consumer.close();
                        }
                    }
                } catch (Exception ex) {
                }
            }
        });
        purge.start();

        for (int i = 0; i < MSG_COUNT; i++) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(payload);
            producer.send(message);
            LOG.info("sent message: {}", i);
        }

        done.countDown();
        purge.join(60000);
        if (purge.isAlive()) {
            fail("Consumer thread should have read initial batch and completed.");
        }

        //wait for processed acked messages
        assertTrue(Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getDequeueCount() == counter.get();
            }
        }));

        long remainingQueued = queueView.getQueueSize();
        LOG.info("Remaining messages to consume: {}", remainingQueued);
        assertEquals(remainingQueued, MSG_COUNT - counter.get());

        MessageConsumer consumer = session.createConsumer(queue);
        for (int i = counter.get(); i < MSG_COUNT; i++) {
            Message message = consumer.receive(5000);
            assertNotNull("Should not get null message", consumer);
            counter.incrementAndGet();
            message.acknowledge();
            LOG.info("Read message: {}", i);
        }

        assertEquals("Should consume all messages", MSG_COUNT, counter.get());
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();

        KahaDBStore persistence = createStore(true);
        persistence.setJournalMaxFileLength(1024 * 1024 * 1);

        answer.setPersistent(true);
        answer.setPersistenceAdapter(persistence);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.getSystemUsage().getMemoryUsage().setLimit(1024 * 1024 * 6);
        answer.getSystemUsage().getTempUsage().setLimit(1024 * 1024 * 5);
        answer.getSystemUsage().getStoreUsage().setLimit(1024 * 1024 * 5);
        answer.setUseJmx(true);
        answer.getManagementContext().setCreateConnector(false);
        answer.setSchedulerSupport(false);
        answer.setAdvisorySupport(false);

        PListStoreImpl tempStore = ((PListStoreImpl)answer.getSystemUsage().getTempUsage().getStore());
        tempStore.setCleanupInterval(10000);
        tempStore.setJournalMaxFileLength(1024 * 1024 * 2);

        PolicyEntry policy = new PolicyEntry();
        policy.setProducerFlowControl(false);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policy);

        answer.setDestinationPolicy(policyMap);

        return answer;
    }

    private KahaDBStore createStore(boolean delete) throws IOException {
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File("target/activemq-data/kahadb"));
        if( delete ) {
            kaha.deleteAllMessages();
        }
        return kaha;
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }
}