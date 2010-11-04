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

import java.lang.management.ManagementFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TopicSubscriber;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import junit.framework.Test;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.PersistenceAdapter;

public class DurableSubscriptionSelectorTest extends org.apache.activemq.TestSupport {

    MBeanServer mbs;
    BrokerService broker = null;
    ActiveMQTopic topic;

    ActiveMQConnection consumerConnection = null, producerConnection = null;
    Session producerSession;
    MessageProducer producer;

    private int received = 0;

    public static Test suite() {
        return suite(DurableSubscriptionSelectorTest.class);
    }

    public void initCombosForTestSubscription() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter", PersistenceAdapterChoice.values());
    }

    public void testSubscription() throws Exception {
        openConsumer();
        for (int i = 0; i < 4000; i++) {
            sendMessage(false);
        }
        Thread.sleep(1000);

        assertEquals("Invalid message received.", 0, received);

        closeProducer();
        closeConsumer();
        stopBroker();

        startBroker(false);
        openConsumer();

        sendMessage(true);
        Thread.sleep(1000);

        assertEquals("Message is not recieved.", 1, received);

        sendMessage(true);
        Thread.sleep(100);

        assertEquals("Message is not recieved.", 2, received);
    }

    private void openConsumer() throws Exception {
        consumerConnection = (ActiveMQConnection) createConnection();
        consumerConnection.setClientID("cliID");
        consumerConnection.start();
        Session session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, "subName", "filter=true", false);

        subscriber.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                received++;
            }
        });
    }

    private void closeConsumer() throws JMSException {
        if (consumerConnection != null)
            consumerConnection.close();
        consumerConnection = null;
    }

    private void sendMessage(boolean filter) throws Exception {
        if (producerConnection == null) {
            producerConnection = (ActiveMQConnection) createConnection();
            producerConnection.start();
            producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = producerSession.createProducer(topic);
        }

        Message message = producerSession.createMessage();
        message.setBooleanProperty("filter", filter);
        producer.send(message);
    }

    private void closeProducer() throws JMSException {
        if (producerConnection != null)
            producerConnection.close();
        producerConnection = null;
    }

    private int getPendingQueueSize() throws Exception {
        ObjectName[] subs = broker.getAdminView().getDurableTopicSubscribers();
        for (ObjectName sub: subs) {
            if ("cliID".equals(mbs.getAttribute(sub, "ClientId"))) {
                Integer size = (Integer) mbs.getAttribute(sub, "PendingQueueSize");
                return size != null ? size : 0;
            }
        }
        assertTrue(false);
        return -1;
    }

    private void startBroker(boolean deleteMessages) throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("test-broker");
        
        if (deleteMessages) {
            broker.setDeleteAllMessagesOnStartup(true);
        }
        setDefaultPersistenceAdapter(broker);
        broker.start();
    }

    private void stopBroker() throws Exception {
        if (broker != null)
            broker.stop();
        broker = null;
    }
    
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://test-broker?jms.watchTopicAdvisories=false&waitForStart=5000&create=false");
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        startBroker(true);
        topic = (ActiveMQTopic) createDestination();
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    @Override
    protected void tearDown() throws Exception {
        stopBroker();
        super.tearDown();
    }
}
