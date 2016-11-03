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
package org.apache.activemq.test;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsTopicSendReceiveWithTwoConnectionsTest extends JmsSendReceiveTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JmsTopicSendReceiveWithTwoConnectionsTest.class);

    protected Connection sendConnection;
    protected Connection receiveConnection;
    protected Session receiveSession;

    /**
     * Sets up a test where the producer and consumer have their own connection.
     *
     * @see junit.framework.TestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        connectionFactory = createConnectionFactory();

        LOG.info("Creating send connection");
        sendConnection = createSendConnection();
        LOG.info("Starting send connection");
        sendConnection.start();

        LOG.info("Creating receive connection");
        receiveConnection = createReceiveConnection();
        LOG.info("Starting receive connection");
        receiveConnection.start();

        LOG.info("Created sendConnection: " + sendConnection);
        LOG.info("Created receiveConnection: " + receiveConnection);

        session = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        receiveSession = receiveConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        LOG.info("Created sendSession: " + session);
        LOG.info("Created receiveSession: " + receiveSession);

        producer = session.createProducer(null);
        producer.setDeliveryMode(deliveryMode);

        LOG.info("Created producer: " + producer + " delivery mode = " + (deliveryMode == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON_PERSISTENT"));

        if (topic) {
            consumerDestination = session.createTopic(getConsumerSubject());
            producerDestination = session.createTopic(getProducerSubject());
        } else {
            consumerDestination = session.createQueue(getConsumerSubject());
            producerDestination = session.createQueue(getProducerSubject());
        }

        LOG.info("Created  consumer destination: " + consumerDestination + " of type: " + consumerDestination.getClass());
        LOG.info("Created  producer destination: " + producerDestination + " of type: " + producerDestination.getClass());

        consumer = createConsumer();
        consumer.setMessageListener(this);

        LOG.info("Started connections");
    }

    protected MessageConsumer createConsumer() throws JMSException {
        return receiveSession.createConsumer(consumerDestination);
    }

    /*
     * @see junit.framework.TestCase#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        session.close();
        receiveSession.close();
        sendConnection.close();
        receiveConnection.close();
    }

    /**
     * Creates a connection.
     *
     * @return Connection
     * @throws Exception
     */
    protected Connection createReceiveConnection() throws Exception {
        return createConnection();
    }

    /**
     * Creates a connection.
     *
     * @return Connection
     * @throws Exception
     */
    protected Connection createSendConnection() throws Exception {
        return createConnection();
    }

    /**
     * Creates an ActiveMQConnectionFactory.
     *
     * @see org.apache.activemq.test.TestSupport#createConnectionFactory()
     */
    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
    }
}
