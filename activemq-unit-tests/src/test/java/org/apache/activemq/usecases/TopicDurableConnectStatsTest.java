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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.Vector;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicDurableConnectStatsTest extends org.apache.activemq.TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(TopicDurableConnectStatsTest.class);
    private BrokerService broker;
    private ActiveMQTopic topic;
    private final Vector<Throwable> exceptions = new Vector<Throwable>();
    private final int messageSize = 4000;
    protected MBeanServerConnection mbeanServer;
    protected String domain = "org.apache.activemq";
    private ActiveMQConnectionFactory connectionFactory = null;
    final int numMessages = 20;

    private static Session session2 = null;

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {

        connectionFactory = new ActiveMQConnectionFactory("vm://" + getName(true));

        ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
        prefetchPolicy.setAll(10);
        connectionFactory.setPrefetchPolicy(prefetchPolicy);

        connectionFactory.setWatchTopicAdvisories(false);
        return connectionFactory;
    }

    @Override
    protected Connection createConnection() throws Exception {
        return createConnection("cliName");
    }

    protected Connection createConnection(String name) throws Exception {
        Connection con = super.createConnection();
        con.setClientID(name);
        con.start();
        return con;
    }

    public static Test suite() {
        return suite(TopicDurableConnectStatsTest.class);
    }

    @Override
    protected void setUp() throws Exception {
        exceptions.clear();
        topic = (ActiveMQTopic) createDestination();

        createBroker();
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        destroyBroker();
    }

    private void createBroker() throws Exception {
        createBroker(true);
    }

    private void createBroker(boolean deleteAllMessages) throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://" + getName(true) + ")");
        broker.setBrokerName(getName(true));
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        broker.setAdvisorySupport(false);
        broker.addConnector("tcp://0.0.0.0:0");

        setDefaultPersistenceAdapter(broker);
        broker.start();
    }

    private void destroyBroker() throws Exception {
        if (broker != null)
            broker.stop();
    }

    protected ObjectName assertRegisteredObjectName(String name) throws MalformedObjectNameException, NullPointerException {
        ObjectName objectName = new ObjectName(name);

        LOG.info("** Looking for " + name);
        try {
            if (mbeanServer.isRegistered(objectName)) {
                LOG.info("Bean Registered: " + objectName);
            } else {
                LOG.info("Couldn't find Mbean! " + objectName);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return objectName;
    }

    public void testPendingTopicStat() throws Exception {

        Connection consumerCon = createConnection("cliId1");
        Session consumerSession = consumerCon.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = consumerSession.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        assertNotNull(consumer1);

        DurableSubscriptionViewMBean subscriber1 = null;

        ObjectName query = new ObjectName(domain + ":type=Broker,brokerName=" + getName(true) + ",destinationType=Topic,destinationName=" + topic.getTopicName() + ",endpoint=Consumer,clientId=cliId1,consumerId=*");

        java.util.Set<ObjectName>set = mbeanServer.queryNames(query,null);

        ObjectName subscriberObjName1 = set.iterator().next();
        subscriber1 = MBeanServerInvocationHandler.newProxyInstance(mbeanServer, subscriberObjName1, DurableSubscriptionViewMBean.class, true);

        LOG.info("Beginning Pending Queue Size count: " + subscriber1.getPendingQueueSize());
        LOG.info("Prefetch Limit: " + subscriber1.getPrefetchSize());

        assertEquals("no pending", 0, subscriber1.getPendingQueueSize());
        assertEquals("Prefetch Limit ", 10, subscriber1.getPrefetchSize());


        Connection producerCon = createConnection("x");
        Session producerSessions = producerCon.createSession(true, Session.AUTO_ACKNOWLEDGE);


        MessageProducer producer = producerSessions.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        int i = 0;
        for (; i < numMessages; i++) {

            if (i == 15) {
                // kill consumer

                LOG.info("Killing consumer at 15");
                consumerSession.close();
                consumerCon.close();
            }

            TextMessage message = producerSessions.createTextMessage(createMessageText(i));
            message.setJMSExpiration(0);
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
            producerSessions.commit();

        }
        LOG.info("Sent " + i + " messages in total");
        producerCon.close();

        LOG.info("Pending Queue Size count: " + subscriber1.getPendingQueueSize());
        assertEquals("pending as expected", 20, subscriber1.getPendingQueueSize());

        LOG.info("Re-connect client and consume messages");
        Connection con2 = createConnection("cliId1");
        session2 = con2.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);


        final Listener listener = new Listener();
        consumer2.setMessageListener(listener);

        assertTrue("received all sent", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return numMessages == listener.count;
            }
        }));

        LOG.info("Received: " + listener.count);

        int pq = subscriber1.getPendingQueueSize();
        LOG.info("Pending Queue Size count: " + pq);
        assertEquals("Pending queue after consumed", 0, pq);

        session2.close();
        con2.close();
        LOG.info("FINAL Pending Queue Size count (after consumer close): " + subscriber1.getPendingQueueSize());
    }


    private String createMessageText(int index) {
        StringBuffer buffer = new StringBuffer(messageSize);
        buffer.append("Message: " + index + " sent at: " + new Date());
        if (buffer.length() > messageSize) {
            return buffer.substring(0, messageSize);
        }
        for (int i = buffer.length(); i < messageSize; i++) {
            buffer.append(' ');
        }
        return buffer.toString();
    }


    public static class Listener implements MessageListener {
        int count = 0;
        String id = null;

        Listener() {
        }

        @Override
        public void onMessage(Message message) {
            count++;
            try {
                session2.commit();
            } catch (JMSException e1) {
                e1.printStackTrace();
            }

            if (id != null) {
                try {
                    LOG.info(id + ", " + message.getJMSMessageID());
                } catch (Exception ignored) {
                }
            }

            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}


