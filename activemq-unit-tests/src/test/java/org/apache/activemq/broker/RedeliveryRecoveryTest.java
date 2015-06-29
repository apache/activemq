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
package org.apache.activemq.broker;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RedeliveryRecoveryTest {

    static final Logger LOG = LoggerFactory.getLogger(RedeliveryRecoveryTest.class);
    ActiveMQConnection connection;
    BrokerService broker = null;
    String queueName = "redeliveryRecoveryQ";

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        configureBroker(broker);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.start();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        broker.stop();
    }

    protected void configureBroker(BrokerService broker) throws Exception {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setPersistJMSRedelivered(true);
        policyMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(policyMap);
        KahaDBPersistenceAdapter kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        kahaDBPersistenceAdapter.setForceRecoverIndex(true);
        broker.addConnector("tcp://0.0.0.0:0");
    }

    @org.junit.Test
    public void testValidateRedeliveryFlagAfterRestart() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString()
                + "?jms.prefetchPolicy.all=0");
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Destination destination = session.createQueue(queueName);
        populateDestination(1, destination, connection);
        MessageConsumer consumer = session.createConsumer(destination);
        Message msg = consumer.receive(5000);
        LOG.info("got: " + msg);
        assertNotNull("got the message", msg);
        assertFalse("got the message", msg.getJMSRedelivered());
        consumer.close();
        connection.close();

        restartBroker();

        connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString()
                + "?jms.prefetchPolicy.all=0");
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        destination = session.createQueue(queueName);
        consumer = session.createConsumer(destination);

        msg = consumer.receive(5000);
        LOG.info("got: " + msg);
        assertNotNull("got the message", msg);
        assertTrue("got the message has redelivered flag", msg.getJMSRedelivered());

        connection.close();
    }


    private void restartBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        broker = createRestartedBroker();
        broker.start();
    }

    private BrokerService createRestartedBroker() throws Exception {
        broker = new BrokerService();
        configureBroker(broker);
        return broker;
    }

    private void populateDestination(final int nbMessages, final Destination destination, javax.jms.Connection connection) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        for (int i = 1; i <= nbMessages; i++) {
            producer.send(session.createTextMessage("<hello id='" + i + "'/>"));
        }
        producer.close();
        session.close();
    }

}