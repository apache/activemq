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
package org.apache.activemq.broker.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Using the broker's scheduler and setting reduceMemoryFootprint="true" causes
 * message properties to be lost.
 */
public class ReduceMemoryFootprintTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReduceMemoryFootprintTest.class);

    private static final String TEST_AMQ_BROKER_URI = "tcp://localhost:0";
    private static final String TEST_QUEUE_NAME = "Reduce.Memory.Footprint.Test";

    private static final String PROP_NAME = "prop_name";
    private static final String PROP_VALUE = "test-value";

    private String connectionURI;
    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        // create a broker
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();

        connectionURI = broker.getTransportConnectorByName("openwire").getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout = 60000)
    public void testPropertyLostNonScheduled() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionURI);
        Connection connection = connectionFactory.createConnection();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(new ActiveMQQueue(TEST_QUEUE_NAME));
        connection.start();

        String messageText = createMessageText();

        ActiveMQTextMessage message = new ActiveMQTextMessage();

        // Try with non-scheduled
        message.setStringProperty(PROP_NAME, PROP_VALUE);

        message.setText(messageText);
        producer.send(message);

        session.commit();

        LOG.info("Attempting to receive non-scheduled message");
        Message receivedMessage = consumeMessages(connection);

        assertNotNull(receivedMessage);
        assertEquals("property should match", PROP_VALUE, receivedMessage.getStringProperty(PROP_NAME));

        connection.close();
    }

    @Test(timeout = 60000)
    public void testPropertyLostScheduled() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionURI);
        Connection connection = connectionFactory.createConnection();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(new ActiveMQQueue(TEST_QUEUE_NAME));
        connection.start();

        String messageText = createMessageText();

        ActiveMQTextMessage message = new ActiveMQTextMessage();

        // Try with scheduled
        message.setStringProperty(PROP_NAME, PROP_VALUE);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, 1000);

        message.setText(messageText);
        producer.send(message);

        session.commit();

        LOG.info("Attempting to receive scheduled message");
        Message receivedMessage = consumeMessages(connection);

        assertNotNull(receivedMessage);
        assertEquals("property should match", PROP_VALUE, receivedMessage.getStringProperty(PROP_NAME));

        connection.close();
    }

    private String createMessageText() {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < 50; i++) {
            buffer.append("1234567890");
        }

        return buffer.toString();
    }

    private Message consumeMessages(Connection connection) {
        Message message = null;

        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(new ActiveMQQueue(TEST_QUEUE_NAME));
            message = consumer.receive(45000);
        } catch (Exception ex) {
            fail("during consume message received exception " + ex.getMessage());
        } finally {
        }

        return message;
    }

    private BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();

        // add the policy entries ~

        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();

        // reduce memory footprint
        pe.setReduceMemoryFootprint(true);
        pe.setOptimizedDispatch(true);

        pe.setQueue(">");
        entries.add(pe);
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);

        broker.deleteAllMessages();
        broker.setSchedulerSupport(true);

        broker.addConnector(TEST_AMQ_BROKER_URI).setName("openwire");

        return broker;
    }
}
