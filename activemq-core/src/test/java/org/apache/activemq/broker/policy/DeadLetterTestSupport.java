/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.broker.policy;

import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.StrictOrderDispatchPolicy;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

/**
 * @version $Revision$
 */
public abstract class DeadLetterTestSupport extends TestSupport {

    protected int messageCount = 10;
    protected long timeToLive = 250;
    protected Connection connection;
    protected Session session;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    private Destination destination;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected boolean durableSubscriber = false;
    protected Destination dlqDestination;
    protected MessageConsumer dlqConsumer;
    protected BrokerService broker;

    protected void setUp() throws Exception {
        super.setUp();
        broker = createBroker();
        broker.start();
        connection = createConnection();
        connection.setClientID(toString());

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
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
        return broker;
    }

    protected void makeConsumer() throws JMSException {
        Destination destination = getDestination();
        if (durableSubscriber) {
            consumer = session.createDurableSubscriber((Topic) destination, destination.toString());
        }
        else {
            consumer = session.createConsumer(destination);
        }
    }

    protected void makeDlqConsumer() throws JMSException {
        dlqDestination = createDlqDestination();

        System.out.println("Consuming from dead letter on: " + dlqDestination);
        dlqConsumer = session.createConsumer(dlqDestination);
    }

    protected void sendMessages() throws JMSException {
        producer = session.createProducer(getDestination());
        producer.setDeliveryMode(deliveryMode);
        producer.setTimeToLive(timeToLive);

        System.out.println("Sending " + messageCount + " messages to: " + getDestination());
        for (int i = 0; i < messageCount; i++) {
            Message message = session.createTextMessage("msg: " + i);
            producer.send(message);
        }
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
