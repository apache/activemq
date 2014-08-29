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

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(value = Parameterized.class)
public class AMQ5212Test {

    BrokerService brokerService;

    @Parameterized.Parameter(0)
    public boolean concurrentStoreAndDispatchQ = true;

    @Parameterized.Parameters(name = "concurrentStoreAndDispatch={0}")
    public static Iterable<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{{Boolean.TRUE}, {Boolean.FALSE}});
    }

    @Before
    public void setUp() throws Exception {
        start(true);
    }

    public void start(boolean deleteAllMessages) throws Exception {
        brokerService = new BrokerService();
        if (deleteAllMessages) {
            brokerService.deleteAllMessages();
        }
        ((KahaDBPersistenceAdapter)brokerService.getPersistenceAdapter()).setConcurrentStoreAndDispatchQueues(concurrentStoreAndDispatchQ);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.setAdvisorySupport(false);
        brokerService.start();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
    }

    @Test
    public void verifyDuplicateSuppressionWithConsumer() throws Exception {
        doVerifyDuplicateSuppression(100, 100, true);
    }

    @Test
    public void verifyDuplicateSuppression() throws Exception {
        doVerifyDuplicateSuppression(100, 100, false);
    }

    public void doVerifyDuplicateSuppression(final int numToSend, final int expectedTotalEnqueue, final boolean demand) throws Exception {
        final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerService.getTransportConnectors().get(0).getPublishableConnectString());
        connectionFactory.setCopyMessageOnSend(false);
        connectionFactory.setWatchTopicAdvisories(false);

        final int concurrency = 40;
        final AtomicInteger workCount = new AtomicInteger(numToSend);
        ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
        for (int i = 0; i < concurrency; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        int i;
                        while ((i = workCount.getAndDecrement()) > 0) {
                            ActiveMQConnection activeMQConnection = (ActiveMQConnection) connectionFactory.createConnection();
                            activeMQConnection.start();
                            ActiveMQSession activeMQSession = (ActiveMQSession) activeMQConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

                            ActiveMQQueue dest = new ActiveMQQueue("queue-" + i + "-"
                                    + AMQ5212Test.class.getSimpleName());
                            ActiveMQMessageProducer activeMQMessageProducer = (ActiveMQMessageProducer) activeMQSession.createProducer(dest);
                            if (demand) {
                                // create demand so page in will happen
                                activeMQSession.createConsumer(dest);
                            }
                            ActiveMQTextMessage message = new ActiveMQTextMessage();
                            message.setDestination(dest);
                            activeMQMessageProducer.send(message, null);

                            // send a duplicate
                            activeMQConnection.syncSendPacket(message);
                            activeMQConnection.close();

                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        TimeUnit.SECONDS.sleep(1);
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.MINUTES);

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return expectedTotalEnqueue == brokerService.getAdminView().getTotalEnqueueCount();
            }
        });
        assertEquals("total enqueue as expected", expectedTotalEnqueue, brokerService.getAdminView().getTotalEnqueueCount());
    }

    @Test
    public void verifyConsumptionOnDuplicate() throws Exception {

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerService.getTransportConnectors().get(0).getPublishableConnectString());
        connectionFactory.setCopyMessageOnSend(false);
        connectionFactory.setWatchTopicAdvisories(false);

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) connectionFactory.createConnection();
        activeMQConnection.start();
        ActiveMQSession activeMQSession = (ActiveMQSession) activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQQueue dest = new ActiveMQQueue("Q");
        ActiveMQMessageProducer activeMQMessageProducer = (ActiveMQMessageProducer) activeMQSession.createProducer(dest);
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setDestination(dest);
        activeMQMessageProducer.send(message, null);

        // send a duplicate
        activeMQConnection.syncSendPacket(message);

        activeMQConnection.close();

        // verify original can be consumed after restart
        brokerService.stop();
        brokerService.start(false);

        connectionFactory = new ActiveMQConnectionFactory(brokerService.getTransportConnectors().get(0).getPublishableConnectString());
        connectionFactory.setCopyMessageOnSend(false);
        connectionFactory.setWatchTopicAdvisories(false);

        activeMQConnection = (ActiveMQConnection) connectionFactory.createConnection();
        activeMQConnection.start();
        activeMQSession = (ActiveMQSession) activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer messageConsumer = activeMQSession.createConsumer(dest);
        Message received = messageConsumer.receive(4000);
        assertNotNull("Got message", received);
        assertEquals("match", message.getJMSMessageID(), received.getJMSMessageID());

        activeMQConnection.close();
    }

    @Test
    public void verifyClientAckConsumptionOnDuplicate() throws Exception {

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerService.getTransportConnectors().get(0).getPublishableConnectString());
        connectionFactory.setCopyMessageOnSend(false);
        connectionFactory.setWatchTopicAdvisories(false);

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) connectionFactory.createConnection();
        activeMQConnection.start();
        ActiveMQSession activeMQSession = (ActiveMQSession) activeMQConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        ActiveMQQueue dest = new ActiveMQQueue("Q");

        MessageConsumer messageConsumer = activeMQSession.createConsumer(dest);

        ActiveMQMessageProducer activeMQMessageProducer = (ActiveMQMessageProducer) activeMQSession.createProducer(dest);
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setDestination(dest);
        activeMQMessageProducer.send(message, null);

        // send a duplicate
        activeMQConnection.syncSendPacket(message);


        Message received = messageConsumer.receive(4000);
        assertNotNull("Got message", received);
        assertEquals("match", message.getJMSMessageID(), received.getJMSMessageID());
        messageConsumer.close();


        messageConsumer = activeMQSession.createConsumer(dest);
        received = messageConsumer.receive(4000);
        assertNotNull("Got message", received);
        assertEquals("match", message.getJMSMessageID(), received.getJMSMessageID());
        received.acknowledge();

        activeMQConnection.close();
    }
}
