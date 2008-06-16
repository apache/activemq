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
package org.apache.activemq.broker.policy;

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

import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision$
 */
public abstract class DeadLetterTestSupport extends TestSupport {
    private static final Log LOG = LogFactory.getLog(DeadLetterTestSupport.class);

    protected int messageCount = 10;
    protected long timeToLive;
    protected Connection connection;
    protected Session session;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected boolean durableSubscriber;
    protected Destination dlqDestination;
    protected MessageConsumer dlqConsumer;
    protected BrokerService broker;
    protected boolean transactedMode;
    protected int acknowledgeMode = Session.CLIENT_ACKNOWLEDGE;
    private Destination destination;

    protected void setUp() throws Exception {
        super.setUp();
        broker = createBroker();
        broker.start();
        connection = createConnection();
        connection.setClientID(toString());

        session = connection.createSession(transactedMode, acknowledgeMode);
        connection.start();
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }

    protected abstract void doTest() throws Exception;

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        PolicyEntry policy = new PolicyEntry();
        DeadLetterStrategy defaultDeadLetterStrategy = policy.getDeadLetterStrategy();
        if(defaultDeadLetterStrategy!=null) {
            defaultDeadLetterStrategy.setProcessNonPersistent(true);
        }
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(pMap);
        return broker;
    }

    protected void makeConsumer() throws JMSException {
        Destination destination = getDestination();
        LOG.info("Consuming from: " + destination);
        if (durableSubscriber) {
            consumer = session.createDurableSubscriber((Topic)destination, destination.toString());
        } else {
            consumer = session.createConsumer(destination);
        }
    }

    protected void makeDlqConsumer() throws JMSException {
        dlqDestination = createDlqDestination();

        LOG.info("Consuming from dead letter on: " + dlqDestination);
        dlqConsumer = session.createConsumer(dlqDestination);
    }

    protected void sendMessages() throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(getDestination());
        producer.setDeliveryMode(deliveryMode);
        producer.setTimeToLive(timeToLive);

        LOG.info("Sending " + messageCount + " messages to: " + getDestination());
        for (int i = 0; i < messageCount; i++) {
            Message message = createMessage(session, i);
            producer.send(message);
        }
    }

    protected TextMessage createMessage(Session session, int i) throws JMSException {
        return session.createTextMessage(getMessageText(i));
    }

    protected String getMessageText(int i) {
        return "message: " + i;
    }

    protected void assertMessage(Message message, int i) throws Exception {
        LOG.info("Received message: " + message);
        assertNotNull("No message received for index: " + i, message);
        assertTrue("Should be a TextMessage not: " + message, message instanceof TextMessage);
        TextMessage textMessage = (TextMessage)message;
        assertEquals("text of message: " + i, getMessageText(i), textMessage.getText());
    }

    protected abstract Destination createDlqDestination();

    public void testTransientTopicMessage() throws Exception {
        super.topic = true;
        deliveryMode = DeliveryMode.NON_PERSISTENT;
        durableSubscriber = true;
        doTest();
    }

    public void testDurableTopicMessage() throws Exception {
        super.topic = true;
        deliveryMode = DeliveryMode.PERSISTENT;
        durableSubscriber = true;
        doTest();
    }

    public void testTransientQueueMessage() throws Exception {
        super.topic = false;
        deliveryMode = DeliveryMode.NON_PERSISTENT;
        durableSubscriber = false;
        doTest();
    }

    public void testDurableQueueMessage() throws Exception {
        super.topic = false;
        deliveryMode = DeliveryMode.PERSISTENT;
        durableSubscriber = false;
        doTest();
    }

    public Destination getDestination() {
        if (destination == null) {
            destination = createDestination();
        }
        return destination;
    }
}
