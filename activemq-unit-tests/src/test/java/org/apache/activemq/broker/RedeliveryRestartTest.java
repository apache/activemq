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

import java.util.Arrays;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicSubscriber;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(value = Parameterized.class)
public class RedeliveryRestartTest extends TestSupport {

    private static final transient Logger LOG = LoggerFactory.getLogger(RedeliveryRestartTest.class);
    ActiveMQConnection connection;
    BrokerService broker = null;
    String queueName = "redeliveryRestartQ";

    @Parameterized.Parameter
    public TestSupport.PersistenceAdapterChoice persistenceAdapterChoice = PersistenceAdapterChoice.KahaDB;

    @Parameterized.Parameters(name="Store={0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{{TestSupport.PersistenceAdapterChoice.KahaDB},{TestSupport.PersistenceAdapterChoice.JDBC},{TestSupport.PersistenceAdapterChoice.LevelDB}});
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        broker = new BrokerService();
        configureBroker(broker);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.start();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        broker.stop();
        super.tearDown();
    }

    protected void configureBroker(BrokerService broker) throws Exception {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setPersistJMSRedelivered(true);
        policyMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(policyMap);
        setPersistenceAdapter(broker, persistenceAdapterChoice);
        broker.addConnector("tcp://0.0.0.0:0");
    }

    @org.junit.Test
    public void testValidateRedeliveryFlagAfterRestartNoTx() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(" + broker.getTransportConnectors().get(0).getPublishableConnectString()
            + ")?jms.prefetchPolicy.all=0");
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Destination destination = session.createQueue(queueName);
        populateDestination(10, destination, connection);

        MessageConsumer consumer = session.createConsumer(destination);
        TextMessage msg = null;
        for (int i = 0; i < 5; i++) {
            msg = (TextMessage) consumer.receive(20000);
            LOG.info("not redelivered? got: " + msg);
            assertNotNull("got the message", msg);
            assertEquals("first delivery", 1, msg.getLongProperty("JMSXDeliveryCount"));
            assertEquals("not a redelivery", false, msg.getJMSRedelivered());
        }
        consumer.close();

        restartBroker();

        // make failover aware of the restarted auto assigned port
        connection.getTransport().narrow(FailoverTransport.class).add(true, broker.getTransportConnectors().get(0)
                .getPublishableConnectString());

        consumer = session.createConsumer(destination);
        for (int i = 0; i < 5; i++) {
            msg = (TextMessage) consumer.receive(4000);
            LOG.info("redelivered? got: " + msg);
            assertNotNull("got the message again", msg);
            assertEquals("re delivery flag", true, msg.getJMSRedelivered());
            assertEquals("redelivery count survives restart", 2, msg.getLongProperty("JMSXDeliveryCount"));
            msg.acknowledge();
        }

        // consume the rest that were not redeliveries
        for (int i = 0; i < 5; i++) {
            msg = (TextMessage) consumer.receive(20000);
            LOG.info("not redelivered? got: " + msg);
            assertNotNull("got the message", msg);
            assertEquals("not a redelivery", false, msg.getJMSRedelivered());
            assertEquals("first delivery", 1, msg.getLongProperty("JMSXDeliveryCount"));
            msg.acknowledge();
        }
        connection.close();
    }

    @org.junit.Test
    public void testDurableSubRedeliveryFlagAfterRestartNotSupported() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(" + broker.getTransportConnectors().get(0).getPublishableConnectString()
            + ")?jms.prefetchPolicy.all=0");
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.setClientID("id");
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        ActiveMQTopic destination = new ActiveMQTopic(queueName);

        TopicSubscriber durableSub = session.createDurableSubscriber(destination, "id");

        populateDestination(10, destination, connection);

        TextMessage msg = null;
        for (int i = 0; i < 5; i++) {
            msg = (TextMessage) durableSub.receive(20000);
            LOG.info("not redelivered? got: " + msg);
            assertNotNull("got the message", msg);
            assertEquals("first delivery", 1, msg.getLongProperty("JMSXDeliveryCount"));
            assertEquals("not a redelivery", false, msg.getJMSRedelivered());
        }
        durableSub.close();

        restartBroker();

        // make failover aware of the restarted auto assigned port
        connection.getTransport().narrow(FailoverTransport.class).add(true, broker.getTransportConnectors().get(0)
                .getPublishableConnectString());

        durableSub = session.createDurableSubscriber(destination, "id");
        for (int i = 0; i < 10; i++) {
            msg = (TextMessage) durableSub.receive(4000);
            LOG.info("redelivered? got: " + msg);
            assertNotNull("got the message again", msg);
            assertEquals("no reDelivery flag", false, msg.getJMSRedelivered());
            msg.acknowledge();
        }
        connection.close();
    }

    @org.junit.Test
    public void testValidateRedeliveryFlagAfterRestart() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(" + broker.getTransportConnectors().get(0).getPublishableConnectString()
            + ")?jms.prefetchPolicy.all=0");
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = session.createQueue(queueName);
        populateDestination(10, destination, connection);

        MessageConsumer consumer = session.createConsumer(destination);
        TextMessage msg = null;
        for (int i = 0; i < 5; i++) {
            msg = (TextMessage) consumer.receive(20000);
            LOG.info("not redelivered? got: " + msg);
            assertNotNull("got the message", msg);
            assertEquals("first delivery", 1, msg.getLongProperty("JMSXDeliveryCount"));
            assertEquals("not a redelivery", false, msg.getJMSRedelivered());
        }
        session.rollback();
        consumer.close();

        restartBroker();

        // make failover aware of the restarted auto assigned port
        connection.getTransport().narrow(FailoverTransport.class).add(true, broker.getTransportConnectors().get(0)
                .getPublishableConnectString());

        consumer = session.createConsumer(destination);
        for (int i = 0; i < 5; i++) {
            msg = (TextMessage) consumer.receive(4000);
            LOG.info("redelivered? got: " + msg);
            assertNotNull("got the message again", msg);
            assertEquals("redelivery count survives restart", 2, msg.getLongProperty("JMSXDeliveryCount"));
            assertEquals("re delivery flag", true, msg.getJMSRedelivered());
        }
        session.commit();

        // consume the rest that were not redeliveries
        for (int i = 0; i < 5; i++) {
            msg = (TextMessage) consumer.receive(20000);
            LOG.info("not redelivered? got: " + msg);
            assertNotNull("got the message", msg);
            assertEquals("first delivery", 1, msg.getLongProperty("JMSXDeliveryCount"));
            assertEquals("not a redelivery", false, msg.getJMSRedelivered());
        }
        session.commit();

        connection.close();
    }

    @org.junit.Test
    public void testValidateRedeliveryFlagAfterRecovery() throws Exception {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString()
            + "?jms.prefetchPolicy.all=0");
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = session.createQueue(queueName);
        populateDestination(1, destination, connection);

        MessageConsumer consumer = session.createConsumer(destination);
        TextMessage msg = (TextMessage) consumer.receive(5000);
        LOG.info("got: " + msg);
        assertNotNull("got the message", msg);
        assertEquals("first delivery", 1, msg.getLongProperty("JMSXDeliveryCount"));
        assertEquals("not a redelivery", false, msg.getJMSRedelivered());

        stopBrokerWithStoreFailure(broker, persistenceAdapterChoice);

        broker = createRestartedBroker();
        broker.start();

        connection.close();

        connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString());
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        session = connection.createSession(true, Session.SESSION_TRANSACTED);
        consumer = session.createConsumer(destination);
        msg = (TextMessage) consumer.receive(10000);
        assertNotNull("got the message again", msg);
        assertEquals("redelivery count survives restart", 2, msg.getLongProperty("JMSXDeliveryCount"));
        assertEquals("re delivery flag", true, msg.getJMSRedelivered());

        session.commit();
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
        for (int i = 1; i <= nbMessages; i++) {
            producer.send(session.createTextMessage("<hello id='" + i + "'/>"));
        }
        producer.close();
        session.close();
    }
}
