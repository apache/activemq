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
package org.apache.activemq.test;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision: 1.2 $
 */
public class JmsTopicSendReceiveTest extends JmsSendReceiveTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(JmsTopicSendReceiveTest.class);

    protected Connection connection;

    protected void setUp() throws Exception {
        super.setUp();

        connectionFactory = createConnectionFactory();
        connection = createConnection();
        if (durable) {
            connection.setClientID(getClass().getName());
        }

        LOG.info("Created connection: " + connection);

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumeSession = createConsumerSession();

        LOG.info("Created session: " + session);
        LOG.info("Created consumeSession: " + consumeSession);
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
        startConnection();

        LOG.info("Created connection: " + connection);
    }

    protected void startConnection() throws JMSException {
        connection.start();
    }

    protected void tearDown() throws Exception {
        LOG.info("Dumping stats...");
        // TODO
        // connectionFactory.getFactoryStats().dump(new IndentPrinter());

        LOG.info("Closing down connection");

        /** TODO we should be able to shut down properly */
        session.close();
        connection.close();
    }

    /**
     * Creates a session.
     * 
     * @return session
     * @throws JMSException
     */
    protected Session createConsumerSession() throws JMSException {
        if (useSeparateSession) {
            return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } else {
            return session;
        }
    }

    /**
     * Creates a durable suscriber or a consumer.
     * 
     * @return MessageConsumer - durable suscriber or consumer.
     * @throws JMSException
     */
    protected MessageConsumer createConsumer() throws JMSException {
        if (durable) {
            LOG.info("Creating durable consumer");
            return consumeSession.createDurableSubscriber((Topic)consumerDestination, getName());
        }
        return consumeSession.createConsumer(consumerDestination);
    }
}
