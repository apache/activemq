/*
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ6293Test {

    static final Logger LOG = LoggerFactory.getLogger(AMQ6293Test.class);

    private BrokerService brokerService;
    private String connectionUri;
    private final ExecutorService service = Executors.newFixedThreadPool(6);
    private final ActiveMQQueue queue = new ActiveMQQueue("test");
    private final int numMessages = 10000;
    private Connection connection;
    private Session session;
    private final AtomicBoolean isException = new AtomicBoolean();

    @Before
    public void before() throws Exception {
        brokerService = new BrokerService();
        TransportConnector connector = brokerService.addConnector("tcp://localhost:0");
        connectionUri = connector.getPublishableConnectString();
        brokerService.setPersistent(false);
        brokerService.getManagementContext().setCreateConnector(false);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        policyMap.setDefaultEntry(entry);
        brokerService.setDestinationPolicy(policyMap);
        entry.setQueuePrefetch(100);

        brokerService.start();
        brokerService.waitUntilStarted();

        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);

        connection = factory.createConnection();
        connection.start();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    public void after() throws Exception {
        if (connection != null) {
            connection.stop();
        }
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    @Test(timeout=90000)
    public void testDestinationStatisticsOnPurge() throws Exception {
        //send messages to the store
        sendTestMessages(numMessages);

        //Start up 5 consumers
        final Queue regionQueue = (Queue) brokerService.getRegionBroker().getDestinationMap().get(queue);
        for (int i = 0; i < 5; i++) {
            service.submit(new TestConsumer(session.createConsumer(queue)));
        }

        //Start a purge task at the same time as the consumers
        for (int i = 0; i < 1; i++) {
            service.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        regionQueue.purge();
                    } catch (Exception e) {
                        isException.set(true);
                        LOG.warn(e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        service.shutdown();
        assertTrue("Took too long to shutdown service", service.awaitTermination(1, TimeUnit.MINUTES));
        assertFalse("Exception encountered", isException.get());

        //Verify dequeue and message counts
        assertEquals(0, regionQueue.getDestinationStatistics().getMessages().getCount());
        assertEquals(numMessages, regionQueue.getDestinationStatistics().getDequeues().getCount());
    }

    private void sendTestMessages(int numMessages) throws JMSException {
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(queue);

        final TextMessage textMessage = session.createTextMessage();
        textMessage.setText("Message");
        for (int i = 1; i <= numMessages; i++) {
            producer.send(textMessage);
            if (i % 1000 == 0) {
                LOG.info("Sent {} messages", i);
                session.commit();
            }
        }

        session.close();
    }

    private class TestConsumer implements Runnable {
        private final MessageConsumer consumer;

        public TestConsumer(final MessageConsumer consumer) throws JMSException {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try {
                int i = 0;
                while (consumer.receive(1000) != null) {
                    i++;
                    if (i % 1000 == 0) {
                        LOG.info("Received {} messages", i);
                    }
                }
            } catch (Exception e) {
                isException.set(true);
                LOG.warn(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    };
}