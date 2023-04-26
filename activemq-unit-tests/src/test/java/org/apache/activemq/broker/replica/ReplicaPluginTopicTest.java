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
package org.apache.activemq.broker.replica;

import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Arrays;
import java.util.UUID;

public class ReplicaPluginTopicTest extends ReplicaPluginTestSupport {

    private static final String CLIENT_ID_ONE = "one";
    private static final String CLIENT_ID_TWO = "two";
    private static final String CLIENT_ID_XA = "xa";

    protected Connection firstBrokerConnection;
    protected Connection secondBrokerConnection;

    protected Connection firstBrokerConnection2;
    protected Connection secondBrokerConnection2;

    protected XAConnection firstBrokerXAConnection;
    protected Connection secondBrokerXAConnection;

    @Override
    protected void setUp() throws Exception {
        useTopic = true;

        super.setUp();
        firstBrokerConnection = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.setClientID(CLIENT_ID_ONE);
        firstBrokerConnection.start();

        secondBrokerConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.setClientID(CLIENT_ID_ONE);
        secondBrokerConnection.start();

        firstBrokerConnection2 = firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection2.setClientID(CLIENT_ID_TWO);
        firstBrokerConnection2.start();

        secondBrokerConnection2 = secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection2.setClientID(CLIENT_ID_TWO);
        secondBrokerConnection2.start();

        firstBrokerXAConnection = firstBrokerXAConnectionFactory.createXAConnection();
        firstBrokerXAConnection.setClientID(CLIENT_ID_XA);
        firstBrokerXAConnection.start();

        secondBrokerXAConnection = secondBrokerConnectionFactory.createConnection();
        secondBrokerXAConnection.setClientID(CLIENT_ID_XA);
        secondBrokerXAConnection.start();
    }

    @Override
    protected void tearDown() throws Exception {
        if (firstBrokerConnection != null) {
            firstBrokerConnection.close();
            firstBrokerConnection = null;
        }
        if (secondBrokerConnection != null) {
            secondBrokerConnection.close();
            secondBrokerConnection = null;
        }

        if (firstBrokerXAConnection != null) {
            firstBrokerXAConnection.close();
            firstBrokerXAConnection = null;
        }
        if (secondBrokerXAConnection != null) {
            secondBrokerXAConnection.close();
            secondBrokerXAConnection = null;
        }

        super.tearDown();
    }

    public void testSendMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testAcknowledgeMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);
        firstBrokerConnection2.createSession(false, Session.CLIENT_ACKNOWLEDGE).createDurableSubscriber((Topic) destination, CLIENT_ID_TWO);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);
        firstBrokerSession.close();

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());
        secondBrokerSession.close();

        firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);
        receivedMessage = firstBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        receivedMessage.acknowledge();

        Thread.sleep(LONG_TIMEOUT);

        secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        receivedMessage = secondBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNull(receivedMessage);
        secondBrokerSession.close();


        secondBrokerSession = secondBrokerConnection2.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_TWO);
        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());
        secondBrokerSession.close();

        firstBrokerSession.close();
        secondBrokerSession.close();
    }


    public void testSendMessageTransactionCommit() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.commit();

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testSendMessageTransactionRollback() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.rollback();

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testSendMessageXATransactionCommit() throws Exception {
        XASession firstBrokerSession = firstBrokerXAConnection.createXASession();
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_XA);

        XAResource xaRes = firstBrokerSession.getXAResource();
        Xid xid = createXid();
        xaRes.start(xid, XAResource.TMNOFLAGS);

        TextMessage message  = firstBrokerSession.createTextMessage(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerXAConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_XA);

        xaRes.end(xid, XAResource.TMSUCCESS);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.prepare(xid);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.commit(xid, false);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testSendMessageXATransactionRollback() throws Exception {
        XASession firstBrokerSession = firstBrokerXAConnection.createXASession();
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_XA);

        XAResource xaRes = firstBrokerSession.getXAResource();
        Xid xid = createXid();
        xaRes.start(xid, XAResource.TMNOFLAGS);

        TextMessage message  = firstBrokerSession.createTextMessage(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT);

        Session secondBrokerSession = secondBrokerXAConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_XA);

        xaRes.end(xid, XAResource.TMSUCCESS);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.prepare(xid);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        xaRes.rollback(xid);

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testExpireMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        firstBrokerProducer.setTimeToLive(LONG_TIMEOUT);

        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);

        Thread.sleep(LONG_TIMEOUT + SHORT_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        Message receivedMessage = secondBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testSendScheduledMessage() throws Exception {
        long delay = 2 * LONG_TIMEOUT;
        long period = SHORT_TIMEOUT;
        int repeat = 2;

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_TWO);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, period);
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, repeat);
        firstBrokerProducer.send(message);

        Message receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNull(receivedMessage); // should not be available before delay time expire

        Thread.sleep(LONG_TIMEOUT);
        Thread.sleep(SHORT_TIMEOUT); // waiting to ensure that message is added to queue after the delay

        receivedMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage); // should be available now
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());
        assertFalse(receivedMessage.propertyExists(ScheduledMessage.AMQ_SCHEDULED_DELAY));
        assertFalse(receivedMessage.propertyExists(ScheduledMessage.AMQ_SCHEDULED_PERIOD));
        assertFalse(receivedMessage.propertyExists(ScheduledMessage.AMQ_SCHEDULED_REPEAT));

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testAcknowledgeScheduledMessage() throws Exception {
        long delay = SHORT_TIMEOUT;
        long period = SHORT_TIMEOUT;
        int repeat = 1;

        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, period);
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, repeat);
        firstBrokerProducer.send(message);

        Thread.sleep(2 * LONG_TIMEOUT); // Waiting for message to be scheduled

        Message receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());
        receivedMessage.acknowledge();

        receivedMessage = firstBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNotNull(receivedMessage);
        assertTrue(receivedMessage instanceof TextMessage);
        assertEquals(getName(), ((TextMessage) receivedMessage).getText());
        receivedMessage.acknowledge();

        firstBrokerSession.close();
        Thread.sleep(SHORT_TIMEOUT);

        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);

        receivedMessage = secondBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNull(receivedMessage);

        firstBrokerSession.close();
        secondBrokerSession.close();
    }

    public void testBrowseMessage() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_XA);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        TopicViewMBean secondBrokerDestinationTopicView = getTopicView(secondBroker, destination.getPhysicalName());
        assertEquals(secondBrokerDestinationTopicView.browseMessages().size(), 1);
        TextMessage destinationMessage = (TextMessage) secondBrokerDestinationTopicView.browseMessages().get(0);
        assertEquals(destinationMessage.getText(), getName());

        assertEquals(secondBrokerDestinationTopicView.getProducerCount(), 0);
        assertEquals(secondBrokerDestinationTopicView.getConsumerCount(), 1);

        Message receivedMessage = firstBrokerConsumer.receive(SHORT_TIMEOUT);
        assertNotNull(receivedMessage);
        receivedMessage.acknowledge();
        Thread.sleep(LONG_TIMEOUT);
        assertEquals(secondBrokerDestinationTopicView.getDequeueCount(), 1);
        firstBrokerSession.close();
    }

    public void testDeleteTopic() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_XA);

        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName());
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        MBeanServer secondBrokerMbeanServer = secondBroker.getManagementContext().getMBeanServer();
        ObjectName secondBrokerViewMBeanName = assertRegisteredObjectName(secondBrokerMbeanServer, secondBroker.getBrokerObjectName().toString());
        BrokerViewMBean secondBrokerMBean = MBeanServerInvocationHandler.newProxyInstance(secondBrokerMbeanServer, secondBrokerViewMBeanName, BrokerViewMBean.class, true);
        assertEquals(Arrays.stream(secondBrokerMBean.getTopics())
            .map(ObjectName::toString)
            .peek(name -> System.out.println("topic name: " + name))
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 1);

        MBeanServer firstBrokerMbeanServer = firstBroker.getManagementContext().getMBeanServer();
        ObjectName firstBrokerViewMBeanName = assertRegisteredObjectName(firstBrokerMbeanServer, firstBroker.getBrokerObjectName().toString());
        BrokerViewMBean firstBrokerMBean = MBeanServerInvocationHandler.newProxyInstance(firstBrokerMbeanServer, firstBrokerViewMBeanName, BrokerViewMBean.class, true);
        firstBrokerMBean.removeTopic(destination.getPhysicalName());
        Thread.sleep(LONG_TIMEOUT);

        assertEquals(Arrays.stream(secondBrokerMBean.getQueues())
            .map(ObjectName::toString)
            .filter(name -> name.contains("destinationName=" + destination.getPhysicalName()))
            .count(), 0);

        firstBrokerSession.close();
    }

    public void testDurableSubscribers() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumerOne = firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);
        ActiveMQTextMessage message  = new ActiveMQTextMessage();
        message.setText(getName() + "No. 1");
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        TopicViewMBean secondBrokerDestinationTopicView = getTopicView(secondBroker, destination.getPhysicalName());
        TopicViewMBean firstBrokerDestinationTopicView = getTopicView(firstBroker, destination.getPhysicalName());
        assertEquals(firstBrokerDestinationTopicView.getConsumerCount(), 1);
        assertEquals(secondBrokerDestinationTopicView.getConsumerCount(), 1);


        firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_TWO);
        message  = new ActiveMQTextMessage();
        message.setText(getName() + "No. 2");
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);
        assertEquals(secondBrokerDestinationTopicView.getConsumerCount(), 2);
        assertEquals(firstBrokerDestinationTopicView.getConsumerCount(), 2);

        firstBrokerConsumerOne.close();
        firstBrokerSession.unsubscribe(CLIENT_ID_ONE);
        message  = new ActiveMQTextMessage();
        message.setText(getName() + "No. 3");
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);
        assertEquals(firstBrokerDestinationTopicView.getConsumerCount(), 1);
        assertEquals(secondBrokerDestinationTopicView.getConsumerCount(), 1);
        firstBrokerSession.close();
    }

    public void testTemporaryTopicIsNotReplicated() throws Exception {
        Session firstBrokerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer firstBrokerProducer = firstBrokerSession.createProducer(destination);
        MessageConsumer firstBrokerConsumer = firstBrokerSession.createDurableSubscriber((Topic) destination, CLIENT_ID_ONE);

        TemporaryTopic temporaryTopic = firstBrokerSession.createTemporaryTopic();
        MessageConsumer firstBrokerTempTopicConsumer = firstBrokerSession.createConsumer(temporaryTopic);
        TextMessage message  = firstBrokerSession.createTextMessage(getName());
        String id = UUID.randomUUID().toString();
        message.setJMSReplyTo(temporaryTopic);
        message.setJMSCorrelationID(id);
        firstBrokerProducer.send(message);
        Thread.sleep(LONG_TIMEOUT);

        Message firstBrokerMessageDestinationReceived = firstBrokerConsumer.receive(LONG_TIMEOUT);
        if (firstBrokerMessageDestinationReceived instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) firstBrokerMessageDestinationReceived;
            Destination replyBackTopic = textMessage.getJMSReplyTo();
            MessageProducer producer = firstBrokerSession.createProducer(replyBackTopic);

            TextMessage msg = firstBrokerSession.createTextMessage("Message Received : " + textMessage.getText());
            producer.send(msg);
        }

        Message firstBrokerMessageReceived = firstBrokerTempTopicConsumer.receive(LONG_TIMEOUT);
        assertNotNull(firstBrokerMessageReceived);
        assertTrue(((TextMessage) firstBrokerMessageReceived).getText().contains(getName()));

        String tempTopicJMXName = temporaryTopic.getTopicName().replaceAll(":", "_");
        MBeanServer firstBrokerMbeanServer = firstBroker.getManagementContext().getMBeanServer();
        ObjectName firstBrokerViewMBeanName = assertRegisteredObjectName(firstBrokerMbeanServer, firstBroker.getBrokerObjectName().toString());
        BrokerViewMBean firstBrokerMBean = MBeanServerInvocationHandler.newProxyInstance(firstBrokerMbeanServer, firstBrokerViewMBeanName, BrokerViewMBean.class, true);
        assertEquals(firstBrokerMBean.getTemporaryTopics().length, 1);
        assertTrue(firstBrokerMBean.getTemporaryTopics()[0].toString().contains(tempTopicJMXName));

        MBeanServer secondBrokerMbeanServer = secondBroker.getManagementContext().getMBeanServer();
        ObjectName secondBrokerViewMBeanName = assertRegisteredObjectName(secondBrokerMbeanServer, secondBroker.getBrokerObjectName().toString());
        BrokerViewMBean secondBrokerMBean = MBeanServerInvocationHandler.newProxyInstance(secondBrokerMbeanServer, secondBrokerViewMBeanName, BrokerViewMBean.class, true);
        assertEquals(secondBrokerMBean.getTemporaryTopics().length, 0);

        firstBrokerSession.close();
    }

    private TopicViewMBean getTopicView(BrokerService broker, String topicName) throws MalformedObjectNameException {
        MBeanServer mbeanServer = broker.getManagementContext().getMBeanServer();
        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Topic,destinationName="+topicName;
        ObjectName topicViewMBeanName = assertRegisteredObjectName(mbeanServer, objectNameStr);
        return MBeanServerInvocationHandler.newProxyInstance(mbeanServer, topicViewMBeanName, TopicViewMBean.class, true);
    }

    private ObjectName assertRegisteredObjectName(MBeanServer mbeanServer, String name) throws MalformedObjectNameException, NullPointerException {
        ObjectName objectName = new ObjectName(name);
        if (mbeanServer.isRegistered(objectName)) {
            System.out.println("Bean Registered: " + objectName);
        } else {
            fail("Could not find MBean!: " + objectName);
        }
        return objectName;
    }

}
