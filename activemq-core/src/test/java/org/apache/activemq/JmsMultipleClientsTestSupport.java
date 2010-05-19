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

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicSubscriber;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.MessageIdList;

/**
 * Test case support used to test multiple message comsumers and message
 * producers connecting to a single broker.
 * 
 * @version $Revision$
 */
public class JmsMultipleClientsTestSupport extends CombinationTestSupport {

    protected Map<MessageConsumer, MessageIdList> consumers = new HashMap<MessageConsumer, MessageIdList>(); // Map of consumer with messages
                                                // received
    protected int consumerCount = 1;
    protected int producerCount = 1;

    protected int messageSize = 1024;

    protected boolean useConcurrentSend = true;
    protected boolean durable;
    public boolean topic;
    protected boolean persistent;

    protected BrokerService broker;
    protected Destination destination;
    protected List<Connection> connections = Collections.synchronizedList(new ArrayList<Connection>());
    protected MessageIdList allMessagesList = new MessageIdList();

    private AtomicInteger producerLock;

    protected void startProducers(Destination dest, int msgCount) throws Exception {
        startProducers(createConnectionFactory(), dest, msgCount);
    }

    protected void startProducers(final ConnectionFactory factory, final Destination dest, final int msgCount) throws Exception {
        // Use concurrent send
        if (useConcurrentSend) {
            producerLock = new AtomicInteger(producerCount);

            for (int i = 0; i < producerCount; i++) {
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        try {
                            sendMessages(factory.createConnection(), dest, msgCount);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        synchronized (producerLock) {
                            producerLock.decrementAndGet();
                            producerLock.notifyAll();
                        }
                    }
                });

                t.start();
            }

            // Wait for all producers to finish sending
            synchronized (producerLock) {
                while (producerLock.get() != 0) {
                    producerLock.wait(2000);
                }
            }

            // Use serialized send
        } else {
            for (int i = 0; i < producerCount; i++) {
                sendMessages(factory.createConnection(), dest, msgCount);
            }
        }
    }

    protected void sendMessages(Connection connection, Destination destination, int count) throws Exception {
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

        for (int i = 0; i < count; i++) {
            TextMessage msg = createTextMessage(session, "" + i);
            producer.send(msg);
        }

        producer.close();
        session.close();
        connection.close();
    }

    protected TextMessage createTextMessage(Session session, String initText) throws Exception {
        TextMessage msg = session.createTextMessage();

        // Pad message text
        if (initText.length() < messageSize) {
            char[] data = new char[messageSize - initText.length()];
            Arrays.fill(data, '*');
            String str = new String(data);
            msg.setText(initText + str);

            // Do not pad message text
        } else {
            msg.setText(initText);
        }

        return msg;
    }

    protected void startConsumers(Destination dest) throws Exception {
        startConsumers(createConnectionFactory(), dest);
    }

    protected void startConsumers(ConnectionFactory factory, Destination dest) throws Exception {
        MessageConsumer consumer;
        for (int i = 0; i < consumerCount; i++) {
            if (durable && topic) {
                consumer = createDurableSubscriber(factory.createConnection(), dest, "consumer" + (i + 1));
            } else {
                consumer = createMessageConsumer(factory.createConnection(), dest);
            }
            MessageIdList list = new MessageIdList();
            list.setParent(allMessagesList);
            consumer.setMessageListener(list);
            consumers.put(consumer, list);
        }
    }

    protected MessageConsumer createMessageConsumer(Connection conn, Destination dest) throws Exception {
        connections.add(conn);

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageConsumer consumer = sess.createConsumer(dest);
        conn.start();

        return consumer;
    }

    protected TopicSubscriber createDurableSubscriber(Connection conn, Destination dest, String name) throws Exception {
        conn.setClientID(name);
        connections.add(conn);
        conn.start();

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final TopicSubscriber consumer = sess.createDurableSubscriber((javax.jms.Topic)dest, name);

        return consumer;
    }

    protected void waitForAllMessagesToBeReceived(int messageCount) throws Exception {
        allMessagesList.waitForMessagesToArrive(messageCount);
    }

    protected ActiveMQDestination createDestination() throws JMSException {
        String name = "." + getClass().getName() + "." + getName();
        // ensure not inadvertently composite because of combos
        name = name.replace(' ','_');
        name = name.replace(',','&');
        if (topic) {
            destination = new ActiveMQTopic("Topic" + name);
            return (ActiveMQDestination)destination;
        } else {
            destination = new ActiveMQQueue("Queue" + name);
            return (ActiveMQDestination)destination;
        }
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost");
    }

    protected BrokerService createBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("broker://()/localhost?persistent=false&useJmx=true"));
    }

    protected void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        broker = createBroker();
        broker.start();
    }

    protected void tearDown() throws Exception {
        for (Iterator<Connection> iter = connections.iterator(); iter.hasNext();) {
            Connection conn = iter.next();
            try {
                conn.close();
            } catch (Throwable e) {
            }
        }
        broker.stop();
        allMessagesList.flushMessages();
        consumers.clear();
        super.tearDown();
    }

    /*
     * Some helpful assertions for multiple consumers.
     */
    protected void assertConsumerReceivedAtLeastXMessages(MessageConsumer consumer, int msgCount) {
        MessageIdList messageIdList = consumers.get(consumer);
        messageIdList.assertAtLeastMessagesReceived(msgCount);
    }

    protected void assertConsumerReceivedAtMostXMessages(MessageConsumer consumer, int msgCount) {
        MessageIdList messageIdList = consumers.get(consumer);
        messageIdList.assertAtMostMessagesReceived(msgCount);
    }

    protected void assertConsumerReceivedXMessages(MessageConsumer consumer, int msgCount) {
        MessageIdList messageIdList = consumers.get(consumer);
        messageIdList.assertMessagesReceivedNoWait(msgCount);
    }

    protected void assertEachConsumerReceivedAtLeastXMessages(int msgCount) {
        for (Iterator<MessageConsumer> i = consumers.keySet().iterator(); i.hasNext();) {
            assertConsumerReceivedAtLeastXMessages(i.next(), msgCount);
        }
    }

    protected void assertEachConsumerReceivedAtMostXMessages(int msgCount) {
        for (Iterator<MessageConsumer> i = consumers.keySet().iterator(); i.hasNext();) {
            assertConsumerReceivedAtMostXMessages(i.next(), msgCount);
        }
    }

    protected void assertEachConsumerReceivedXMessages(int msgCount) {
        for (Iterator<MessageConsumer> i = consumers.keySet().iterator(); i.hasNext();) {
            assertConsumerReceivedXMessages(i.next(), msgCount);
        }
    }

    protected void assertTotalMessagesReceived(int msgCount) {
        allMessagesList.assertMessagesReceivedNoWait(msgCount);

        // now lets count the individual messages received
        int totalMsg = 0;
        for (Iterator<MessageConsumer> i = consumers.keySet().iterator(); i.hasNext();) {
            MessageIdList messageIdList = consumers.get(i.next());
            totalMsg += messageIdList.getMessageCount();
        }
        assertEquals("Total of consumers message count", msgCount, totalMsg);
    }
}
