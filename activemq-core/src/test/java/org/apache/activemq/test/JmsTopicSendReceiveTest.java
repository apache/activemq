/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * @version $Revision: 1.2 $
 */
public class JmsTopicSendReceiveTest extends JmsSendReceiveTestSupport {
    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(JmsTopicSendReceiveTest.class);
    
    protected Connection connection;

    protected void setUp() throws Exception {
        super.setUp();

        connectionFactory = createConnectionFactory();
        connection = createConnection();
        if (durable) {
            connection.setClientID(getClass().getName());
        }

        log.info("Created connection: " + connection);

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumeSession = createConsumerSession();

        log.info("Created session: " + session);
        log.info("Created consumeSession: " + consumeSession);
        producer = session.createProducer(null);
        producer.setDeliveryMode(deliveryMode);

        log.info("Created producer: " + producer + " delivery mode = " +
                (deliveryMode == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON_PERSISTENT"));

        if (topic) {
            consumerDestination = session.createTopic(getConsumerSubject());
            producerDestination = session.createTopic(getProducerSubject());
        }
        else {
            consumerDestination = session.createQueue(getConsumerSubject());
            producerDestination = session.createQueue(getProducerSubject());
        }

        log.info("Created  consumer destination: " + consumerDestination + " of type: " + consumerDestination.getClass());
        log.info("Created  producer destination: " + producerDestination + " of type: " + producerDestination.getClass());
        consumer = createConsumer();
        consumer.setMessageListener(this);
        startConnection();

        log.info("Created connection: " + connection);
    }

    protected void startConnection() throws JMSException {
        connection.start();
    }

    protected void tearDown() throws Exception {
        log.info("Dumping stats...");
        //TODO
        //connectionFactory.getFactoryStats().dump(new IndentPrinter());

        log.info("Closing down connection");

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
        }
        else {
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
            log.info("Creating durable consumer");
            return consumeSession.createDurableSubscriber((Topic) consumerDestination, getName());
        }
        return consumeSession.createConsumer(consumerDestination);
    }
}
