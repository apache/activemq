/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
**/
package org.activemq.test;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Session;

import org.activemq.ActiveMQConnectionFactory;

/**
 * @version $Revision: 1.3 $
 */
public class JmsTopicSendReceiveWithTwoConnectionsTest extends JmsSendReceiveTestSupport {
    
    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
            .getLog(JmsTopicSendReceiveWithTwoConnectionsTest.class);

    protected Connection sendConnection;
    protected Connection receiveConnection;
    protected Session receiveSession;

    /**
     * Sets up a test where the producer and consumer have their own connection.
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();

        connectionFactory = createConnectionFactory();

        sendConnection = createSendConnection();
        sendConnection.start();

        receiveConnection = createReceiveConnection();
        receiveConnection.start();

        log.info("Created sendConnection: " + sendConnection);
        log.info("Created receiveConnection: " + receiveConnection);

        session = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        receiveSession = receiveConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        log.info("Created sendSession: " + session);
        log.info("Created receiveSession: " + receiveSession);

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

        consumer = receiveSession.createConsumer(consumerDestination);
        consumer.setMessageListener(this);

        log.info("Started connections");
    }
    
    /*
     * @see junit.framework.TestCase#tearDown()
     */
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
     * @see org.activemq.test.TestSupport#createConnectionFactory()
     */
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
    }
}