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
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedeliveryRestartTest extends BrokerRestartTestSupport {
    private static final transient Logger LOG = LoggerFactory.getLogger(RedeliveryRestartTest.class);

    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        super.configureBroker(broker);
        KahaDBPersistenceAdapter kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        kahaDBPersistenceAdapter.setRewriteOnRedelivery(true);
        broker.addConnector("tcp://0.0.0.0:0");
    }

    public void testValidateRedeliveryFlagAfterRestart() throws Exception {

        ConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory("failover:(" + broker.getTransportConnectors().get(0).getPublishableConnectString() + ")?jms.immediateAck=true");
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        populateDestination(10, queueName, connection);

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = session.createQueue(queueName);

        MessageConsumer consumer = session.createConsumer(destination);
        TextMessage msg = null;
        for (int i=0; i<5;i++) {
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
        ((FailoverTransport) connection.getTransport().narrow(FailoverTransport.class)).add(true, broker.getTransportConnectors().get(0).getPublishableConnectString());

        consumer = session.createConsumer(destination);
        for (int i=0; i<5;i++) {
            msg = (TextMessage) consumer.receive(4000);
            LOG.info("redelivered? got: " + msg);
            assertNotNull("got the message again", msg);
            assertEquals("redelivery count survives restart", 2, msg.getLongProperty("JMSXDeliveryCount"));
            assertEquals("re delivery flag", true, msg.getJMSRedelivered());
        }
        session.commit();

        // consume the rest that were not redeliveries
        for (int i=0; i<5;i++) {
            msg = (TextMessage) consumer.receive(20000);
            LOG.info("not redelivered? got: " + msg);
            assertNotNull("got the message", msg);
            assertEquals("first delivery", 1, msg.getLongProperty("JMSXDeliveryCount"));
            assertEquals("not a redelivery", false, msg.getJMSRedelivered());
        }
        session.commit();

        connection.close();
    }

    public void testValidateRedeliveryFlagAfterRecovery() throws Exception {
        ConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString() + "?jms.immediateAck=true");
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();

        populateDestination(1, queueName, connection);

        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = session.createQueue(queueName);

        MessageConsumer consumer = session.createConsumer(destination);
        TextMessage msg = (TextMessage) consumer.receive(20000);
        LOG.info("got: " + msg);
        assertNotNull("got the message", msg);
        assertEquals("first delivery", 1, msg.getLongProperty("JMSXDeliveryCount"));
        assertEquals("not a redelivery", false, msg.getJMSRedelivered());

        KahaDBPersistenceAdapter kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();

        // have the broker stop with an IOException on next checkpoint so it has a pending local transaction to recover
        kahaDBPersistenceAdapter.getStore().getJournal().close();
        broker.waitUntilStopped();

        broker = createRestartedBroker();
        broker.start();

        connectionFactory =
                new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString() + "?jms.immediateAck=true");
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

    private void populateDestination(final int nbMessages,
                                     final String destinationName, javax.jms.Connection connection)
            throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(destinationName);
        MessageProducer producer = session.createProducer(destination);
        for (int i = 1; i <= nbMessages; i++) {
            producer.send(session.createTextMessage("<hello id='" + i + "'/>"));
        }
        producer.close();
        session.close();
    }


    public static Test suite() {
        return suite(RedeliveryRestartTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
