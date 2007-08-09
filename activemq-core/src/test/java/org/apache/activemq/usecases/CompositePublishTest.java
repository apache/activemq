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

import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.test.JmsSendReceiveTestSupport;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class CompositePublishTest extends JmsSendReceiveTestSupport {

    protected Connection sendConnection;
    protected Connection receiveConnection;
    protected Session receiveSession;
    protected MessageConsumer[] consumers;
    protected List[] messageLists;

    protected void setUp() throws Exception {
        super.setUp();

        connectionFactory = createConnectionFactory();

        sendConnection = createConnection();
        sendConnection.start();

        receiveConnection = createConnection();
        receiveConnection.start();

        LOG.info("Created sendConnection: " + sendConnection);
        LOG.info("Created receiveConnection: " + receiveConnection);

        session = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        receiveSession = receiveConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        LOG.info("Created sendSession: " + session);
        LOG.info("Created receiveSession: " + receiveSession);

        producer = session.createProducer(null);

        LOG.info("Created producer: " + producer);

        if (topic) {
            consumerDestination = session.createTopic(getConsumerSubject());
            producerDestination = session.createTopic(getProducerSubject());
        } else {
            consumerDestination = session.createQueue(getConsumerSubject());
            producerDestination = session.createQueue(getProducerSubject());
        }

        LOG.info("Created  consumer destination: " + consumerDestination + " of type: " + consumerDestination.getClass());
        LOG.info("Created  producer destination: " + producerDestination + " of type: " + producerDestination.getClass());

        Destination[] destinations = getDestinations();
        consumers = new MessageConsumer[destinations.length];
        messageLists = new List[destinations.length];
        for (int i = 0; i < destinations.length; i++) {
            Destination dest = destinations[i];
            messageLists[i] = createConcurrentList();
            consumers[i] = receiveSession.createConsumer(dest);
            consumers[i].setMessageListener(createMessageListener(i, messageLists[i]));
        }

        LOG.info("Started connections");
    }

    protected MessageListener createMessageListener(int i, final List messageList) {
        return new MessageListener() {
            public void onMessage(Message message) {
                consumeMessage(message, messageList);
            }
        };
    }

    /**
     * Returns the subject on which we publish
     */
    protected String getSubject() {
        return getPrefix() + "FOO.BAR," + getPrefix() + "FOO.X.Y";
    }

    /**
     * Returns the destinations to which we consume
     */
    protected Destination[] getDestinations() {
        return new Destination[] {new ActiveMQTopic(getPrefix() + "FOO.BAR"), new ActiveMQTopic(getPrefix() + "FOO.*"), new ActiveMQTopic(getPrefix() + "FOO.X.Y")};
    }

    protected String getPrefix() {
        return super.getSubject() + ".";
    }

    protected void assertMessagesAreReceived() throws JMSException {
        waitForMessagesToBeDelivered();

        for (int i = 0, size = messageLists.length; i < size; i++) {
            LOG.info("Message list: " + i + " contains: " + messageLists[i].size() + " message(s)");
        }

        for (int i = 0, size = messageLists.length; i < size; i++) {
            assertMessagesReceivedAreValid(messageLists[i]);
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() {
        return new ActiveMQConnectionFactory("vm://localhost");
    }

    protected void tearDown() throws Exception {
        session.close();
        receiveSession.close();

        sendConnection.close();
        receiveConnection.close();
    }
}
