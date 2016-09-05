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
package org.apache.activemq.broker.region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DestinationGCTest {

    protected static final Logger logger = LoggerFactory.getLogger(DestinationGCTest.class);

    private final ActiveMQQueue queue = new ActiveMQQueue("TEST");
    private final ActiveMQQueue otherQueue = new ActiveMQQueue("TEST-OTHER");

    private BrokerService brokerService;

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    protected BrokerService createBroker() throws Exception {
        PolicyEntry entry = new PolicyEntry();
        entry.setGcInactiveDestinations(true);
        entry.setInactiveTimeoutBeforeGC(3000);
        PolicyMap map = new PolicyMap();
        map.setDefaultEntry(entry);

        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        broker.setDestinations(new ActiveMQDestination[] {queue});
        broker.setSchedulePeriodForDestinationPurge(1000);
        broker.setMaxPurgedDestinationsPerSweep(1);
        broker.setDestinationPolicy(map);

        return broker;
    }

    @Test(timeout = 60000)
    public void testDestinationGCWithActiveConsumers() throws Exception {
        assertEquals(1, brokerService.getAdminView().getQueues().length);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createProducer(otherQueue).close();
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
            }
        });

        connection.start();

        assertTrue("After GC runs there should be one Queue.", Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getQueues().length == 1;
            }
        }));

        connection.close();
    }

    @Test(timeout = 60000)
    public void testDestinationGc() throws Exception {
        assertEquals(1, brokerService.getAdminView().getQueues().length);
        assertTrue("After GC runs the Queue should be empty.", Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getQueues().length == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testDestinationGcLimit() throws Exception {

        brokerService.getAdminView().addQueue("TEST1");
        brokerService.getAdminView().addQueue("TEST2");
        brokerService.getAdminView().addQueue("TEST3");
        brokerService.getAdminView().addQueue("TEST4");

        assertEquals(5, brokerService.getAdminView().getQueues().length);
        Thread.sleep(7000);

        int queues = brokerService.getAdminView().getQueues().length;
        assertTrue(queues > 0 && queues < 5);

        assertTrue("After GC runs the Queue should be empty.", Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getQueues().length == 0;
            }
        }));
    }

    @Test(timeout = 60000)
    public void testDestinationGcAnonymousProducer() throws Exception {

        final ActiveMQQueue q = new ActiveMQQueue("Q.TEST.ANONYMOUS.PRODUCER");

        brokerService.getAdminView().addQueue(q.getPhysicalName());
        assertEquals(2, brokerService.getAdminView().getQueues().length);

        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        final Connection connection = factory.createConnection();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // wait for the queue to be marked for GC
        logger.info("Waiting for '{}' to be marked for GC...", q);
        Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getDestination(q).canGC();
            }
        }, Wait.MAX_WAIT_MILLIS, 500L);

        // create anonymous producer and send a message
        logger.info("Sending PERSISTENT message to QUEUE '{}'", q.getPhysicalName());
        final MessageProducer producer = session.createProducer(null);
        producer.send(q, session.createTextMessage());
        producer.close();

        assertFalse(brokerService.getDestination(q).canGC());

        connection.close();
    }
}
