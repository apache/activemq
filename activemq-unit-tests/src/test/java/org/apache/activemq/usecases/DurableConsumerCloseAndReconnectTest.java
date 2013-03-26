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
package org.apache.activemq.usecases;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.test.TestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 
 */
public class DurableConsumerCloseAndReconnectTest extends TestSupport {
    protected static final long RECEIVE_TIMEOUT = 5000L;
    private static final Logger LOG = LoggerFactory.getLogger(DurableConsumerCloseAndReconnectTest.class);

    BrokerService brokerService;

    protected Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private MessageProducer producer;
    private Destination destination;
    private int messageCount;

    private String vmConnectorURI;

    
    @Override
    protected void setUp() throws Exception {
        createBroker();
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        stopBroker();
        super.tearDown();
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(vmConnectorURI);
    }

    protected void createBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setUseJmx(false);
        brokerService.setPersistent(false);
        KahaDBPersistenceAdapter store = new KahaDBPersistenceAdapter();
        brokerService.setPersistenceAdapter(store);
        brokerService.start();
        brokerService.waitUntilStarted();
        vmConnectorURI = brokerService.getVmConnectorURI().toString();
    }

    protected void stopBroker() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    public void testDurableSubscriberReconnectMultipleTimes() throws Exception {
        Connection dummyConnection = createConnection();
        dummyConnection.start();

        makeConsumer(Session.AUTO_ACKNOWLEDGE);
        closeConsumer();

        publish(30);

        int counter = 1;
        for (int i = 0; i < 15; i++) {
            makeConsumer(Session.AUTO_ACKNOWLEDGE);
            Message message = consumer.receive(RECEIVE_TIMEOUT);
            assertTrue("Should have received a message!", message != null);
            LOG.info("Received message " + counter++);
            message = consumer.receive(RECEIVE_TIMEOUT);
            assertTrue("Should have received a message!", message != null);
            LOG.info("Received message " + counter++);
            closeConsumer();
        }

        dummyConnection.close();
    }

    public void testCreateDurableConsumerCloseThenReconnect() throws Exception {
        // force the server to stay up across both connection tests
        Connection dummyConnection = createConnection();
        dummyConnection.start();

        consumeMessagesDeliveredWhileConsumerClosed();

        dummyConnection.close();

        // now lets try again without one connection open
        consumeMessagesDeliveredWhileConsumerClosed();       
    }

    protected void consumeMessagesDeliveredWhileConsumerClosed() throws Exception {
        // default to client ack for consumer
        makeConsumer();
        closeConsumer();

        publish(1);

        // wait a few moments for the close to really occur
        Thread.sleep(1000);

        makeConsumer();

        Message message = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue("Should have received a message!", message != null);

        closeConsumer();

        LOG.info("Now lets create the consumer again and because we didn't ack, we should get it again");
        makeConsumer();

        message = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue("Should have received a message!", message != null);
        message.acknowledge();

        closeConsumer();

        LOG.info("Now lets create the consumer again and because we did ack, we should not get it again");
        makeConsumer();

        message = consumer.receive(2000);
        assertTrue("Should have no more messages left!", message == null);

        closeConsumer();

        LOG.info("Lets publish one more message now");
        publish(1);

        makeConsumer();
        message = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue("Should have received a message!", message != null);
        message.acknowledge();

        closeConsumer();
    }

    protected void publish(int numMessages) throws Exception {
        connection = createConnection();
        connection.start();

        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        destination = createDestination();

        producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        for (int i = 0; i < numMessages; i++) {
            TextMessage msg = session.createTextMessage("This is a test: " + messageCount++);
            producer.send(msg);
        }

        producer.close();
        producer = null;
        closeSession();
    }

    protected Destination createDestination() throws JMSException {
        if (isTopic()) {
            return session.createTopic(getSubject());
        } else {
            return session.createQueue(getSubject());
        }
    }

    protected boolean isTopic() {
        return true;
    }

    protected void closeConsumer() throws JMSException {
        LOG.info("Closing the consumer");
        consumer.close();
        consumer = null;
        closeSession();
    }

    protected void closeSession() throws JMSException {
        session.close();
        session = null;
        connection.close();
        connection = null;
    }

    protected void makeConsumer() throws Exception {
        makeConsumer(Session.CLIENT_ACKNOWLEDGE);
    }

    protected void makeConsumer(int ackMode) throws Exception {
        String durableName = getName();
        String clientID = getSubject();
        LOG.info("Creating a durable subscriber for clientID: " + clientID + " and durable name: " + durableName);
        createSession(clientID, ackMode);
        consumer = createConsumer(durableName);
    }

    private MessageConsumer createConsumer(String durableName) throws JMSException {
        if (destination instanceof Topic) {
            return session.createDurableSubscriber((Topic)destination, durableName);
        } else {
            return session.createConsumer(destination);
        }
    }

    protected void createSession(String clientID, int ackMode) throws Exception {
        connection = createConnection();
        connection.setClientID(clientID);
        connection.start();

        session = connection.createSession(false, ackMode);
        destination = createDestination();
    }
}
