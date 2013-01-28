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
package org.apache.activemq;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

/**
 * @version
 */
public class JmsTopicSendReceiveWithTwoConnectionsTest extends JmsSendReceiveTestSupport {

    private static final org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
        .getLog(JmsTopicSendReceiveWithTwoConnectionsTest.class);

    protected Connection sendConnection;
    protected Connection receiveConnection;
    protected Session receiveSession;

    protected void setUp() throws Exception {
        super.setUp();

        connectionFactory = createConnectionFactory();

        sendConnection = createSendConnection();
        sendConnection.start();

        receiveConnection = createReceiveConnection();
        receiveConnection.start();

        LOG.info("Created sendConnection: " + sendConnection);
        LOG.info("Created receiveConnection: " + receiveConnection);

        session = createSendSession(sendConnection);
        receiveSession = createReceiveSession(receiveConnection);

        LOG.info("Created sendSession: " + session);
        LOG.info("Created receiveSession: " + receiveSession);

        producer = session.createProducer(null);
        producer.setDeliveryMode(deliveryMode);

        LOG.info("Created producer: " + producer + " delivery mode = "
                 + (deliveryMode == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON_PERSISTENT"));

        if (topic) {
            consumerDestination = session.createTopic(getConsumerSubject());
            producerDestination = session.createTopic(getProducerSubject());
        } else {
            consumerDestination = session.createQueue(getConsumerSubject());
            producerDestination = session.createQueue(getProducerSubject());
        }

        LOG.info("Created  consumer destination: " + consumerDestination + " of type: "
                 + consumerDestination.getClass());
        LOG.info("Created  producer destination: " + producerDestination + " of type: "
                 + producerDestination.getClass());

        consumer = createConsumer(receiveSession, consumerDestination);
        consumer.setMessageListener(this);

        LOG.info("Started connections");
    }

    protected Session createReceiveSession(Connection receiveConnection) throws Exception {
        return receiveConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    protected Session createSendSession(Connection sendConnection) throws Exception {
        return sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    protected Connection createReceiveConnection() throws Exception {
        return createConnection();
    }

    protected Connection createSendConnection() throws Exception {
        return createConnection();
    }

    protected MessageConsumer createConsumer(Session session, Destination dest) throws JMSException {
        return session.createConsumer(dest);
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
    }

    protected void tearDown() throws Exception {
        session.close();
        receiveSession.close();
        sendConnection.close();
        receiveConnection.close();
    }
}
