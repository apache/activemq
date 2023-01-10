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
package org.apache.activemq.advisory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.function.Function;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.ConstantPendingMessageLimitStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageDispatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test for advisory messages sent under the right circumstances.
 */
@RunWith(Parameterized.class)
public class AdvisoryTests {

    protected static final int MESSAGE_COUNT = 100;
    protected BrokerService broker;
    protected Connection connection;
    protected String bindAddress = ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL;
    protected final boolean includeBodyForAdvisory;
    protected final boolean persistent;
    protected final int EXPIRE_MESSAGE_PERIOD = 3000;
    protected final int DEFAULT_PREFETCH = 50;

    @Parameters(name = "includeBodyForAdvisory={0}, persistent={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                // Include the full body of the message
                {true, false},
                // Don't include the full body of the message
                {false, false},
                // Include the full body of the message
                {true, true},
                // Don't include the full body of the message
                {false, true}
        });
    }

    public AdvisoryTests(boolean includeBodyForAdvisory, boolean persistent) {
        super();
        this.includeBodyForAdvisory = includeBodyForAdvisory;
        this.persistent = persistent;
    }

    @Test(timeout = 60000)
    public void testNoSlowConsumerAdvisory() throws Exception {
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = s.createQueue(getClass().getName());
        MessageConsumer consumer = s.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
            }
        });

        Topic advisoryTopic = AdvisorySupport.getSlowConsumerAdvisoryTopic((ActiveMQDestination) queue);
        s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        // start throwing messages at the consumer
        MessageProducer producer = s.createProducer(queue);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(new byte[1024]);
            producer.send(m);
        }
        Message msg = advisoryConsumer.receive(1000);
        assertNull(msg);
    }

    @Test(timeout = 60000)
    public void testQueueSlowConsumerAdvisory() throws Exception {
        testSlowConsumerAdvisory(new ActiveMQQueue(getClass().getName()));
    }

    @Test(timeout = 60000)
    public void testTopicSlowConsumerAdvisory() throws Exception {
        broker.getDestinationPolicy().getDefaultEntry().setTopicPrefetch(10);
        broker.getDestinationPolicy().getDefaultEntry().setPendingMessageLimitStrategy(null);
        testSlowConsumerAdvisory(new ActiveMQTopic(getClass().getName()));
    }

    @Test(timeout = 60000)
    public void testDurableSlowConsumerAdvisory() throws Exception {
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = s.createTopic(getClass().getName());
        MessageConsumer consumer = s.createDurableSubscriber(topic, "sub1");
        assertNotNull(consumer);

        Topic advisoryTopic = AdvisorySupport.getSlowConsumerAdvisoryTopic((ActiveMQDestination) topic);
        s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        // start throwing messages at the consumer
        MessageProducer producer = s.createProducer(topic);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(new byte[1024]);
            producer.send(m);
        }
        Message msg = advisoryConsumer.receive(1000);
        assertNotNull(msg);
    }

    private void testSlowConsumerAdvisory(Destination dest) throws Exception {
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = s.createConsumer(dest);
        assertNotNull(consumer);

        Topic advisoryTopic = AdvisorySupport.getSlowConsumerAdvisoryTopic((ActiveMQDestination) dest);
        s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        // start throwing messages at the consumer
        MessageProducer producer = s.createProducer(dest);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(new byte[1024]);
            producer.send(m);
        }
        Message msg = advisoryConsumer.receive(1000);
        assertNotNull(msg);
    }

    @Test(timeout = 60000)
    public void testQueueMessageDeliveryAdvisory() throws Exception {
        testMessageConsumerAdvisory(new ActiveMQQueue(getClass().getName()), AdvisorySupport::getMessageDeliveredAdvisoryTopic, false);
    }

    @Test(timeout = 60000)
    public void testQueueMessageDeliveryAdvisoryTransacted() throws Exception {
        testMessageConsumerAdvisory(new ActiveMQQueue(getClass().getName()), AdvisorySupport::getMessageDeliveredAdvisoryTopic, true);
    }

    @Test(timeout = 60000)
    public void testQueueMessageDispatchedAdvisory() throws Exception {
        testMessageConsumerAdvisory(new ActiveMQQueue(getClass().getName()), AdvisorySupport::getMessageDispatchedAdvisoryTopic, false);
    }

    @Test(timeout = 60000)
    public void testQueueMessageDispatchedAdvisoryTransacted() throws Exception {
        testMessageConsumerAdvisory(new ActiveMQQueue(getClass().getName()), AdvisorySupport::getMessageDispatchedAdvisoryTopic, true);
    }

    @Test(timeout = 60000)
    public void testQueueMessageDispatchedAdvisorySync() throws Exception {
        ((ActiveMQConnection)connection).setDispatchAsync(false);
        testMessageConsumerAdvisory(new ActiveMQQueue(getClass().getName()), AdvisorySupport::getMessageDispatchedAdvisoryTopic, false);
    }

    @Test(timeout = 60000)
    public void testQueueMessageDispatchedAdvisorySyncTransacted() throws Exception {
        ((ActiveMQConnection)connection).setDispatchAsync(false);
        testMessageConsumerAdvisory(new ActiveMQQueue(getClass().getName()), AdvisorySupport::getMessageDispatchedAdvisoryTopic, true);
    }

    @Test(timeout = 60000)
    public void testTopicMessageDeliveryAdvisory() throws Exception {
        testMessageConsumerAdvisory(new ActiveMQTopic(getClass().getName()), AdvisorySupport::getMessageDeliveredAdvisoryTopic, false);
    }

    @Test(timeout = 60000)
    public void testTopicMessageDeliveryAdvisoryTransacted() throws Exception {
        testMessageConsumerAdvisory(new ActiveMQTopic(getClass().getName()), AdvisorySupport::getMessageDeliveredAdvisoryTopic, true);
    }

    @Test(timeout = 60000)
    public void testTopicMessageDispatchedAdvisory() throws Exception {
        testMessageConsumerAdvisory(new ActiveMQTopic(getClass().getName()), AdvisorySupport::getMessageDispatchedAdvisoryTopic, false);
    }

    @Test(timeout = 60000)
    public void testTopicMessageDispatchedAdvisoryTransacted() throws Exception {
        testMessageConsumerAdvisory(new ActiveMQTopic(getClass().getName()), AdvisorySupport::getMessageDispatchedAdvisoryTopic, true);
    }

    @Test(timeout = 60000)
    public void testTopicMessageDispatchedAdvisorySync() throws Exception {
        ((ActiveMQConnection)connection).setDispatchAsync(false);
        testMessageConsumerAdvisory(new ActiveMQTopic(getClass().getName()), AdvisorySupport::getMessageDispatchedAdvisoryTopic, false);
    }

    @Test(timeout = 60000)
    public void testTopicMessageDispatchedAdvisorySyncTransacted() throws Exception {
        ((ActiveMQConnection)connection).setDispatchAsync(false);
        testMessageConsumerAdvisory(new ActiveMQTopic(getClass().getName()), AdvisorySupport::getMessageDispatchedAdvisoryTopic, true);
    }

    @Test(timeout = 60000)
    public void testDurableMessageDispatchedAdvisory() throws Exception {
        testDurableSubscriptionAdvisory(AdvisorySupport::getMessageDispatchedAdvisoryTopic, false);
    }

    @Test(timeout = 60000)
    public void testDurableMessageDispatchedAdvisorySync() throws Exception {
        ((ActiveMQConnection)connection).setDispatchAsync(false);
        testDurableSubscriptionAdvisory(AdvisorySupport::getMessageDispatchedAdvisoryTopic, false);
    }

    @Test(timeout = 60000)
    public void testDurableMessageDispatchedAdvisoryTransacted() throws Exception {
        testDurableSubscriptionAdvisory(AdvisorySupport::getMessageDispatchedAdvisoryTopic, true);
    }

    @Test(timeout = 60000)
    public void testDurableMessageDispatchedAdvisorySyncTransacted() throws Exception {
        ((ActiveMQConnection)connection).setDispatchAsync(false);
        testDurableSubscriptionAdvisory(AdvisorySupport::getMessageDispatchedAdvisoryTopic, true);
    }

    @Test(timeout = 60000)
    public void testQueueBrowserDispatchedAdvisoryNotSent() throws Exception {
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = s.createQueue(getClass().getName());

        Topic advisoryTopic = AdvisorySupport.getMessageDispatchedAdvisoryTopic(
            (ActiveMQDestination) queue);

        //Test util method
        assertTrue(AdvisorySupport.isMessageDispatchedAdvisoryTopic(advisoryTopic));

        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        // start throwing messages at the consumer
        MessageProducer producer = s.createProducer(queue);

        BytesMessage m = s.createBytesMessage();
        m.writeBytes(new byte[1024]);
        producer.send(m);

        QueueBrowser browser = s.createBrowser(queue);
        Enumeration enumeration = browser.getEnumeration();
        //Should have 1 message to browser
        assertTrue(enumeration.hasMoreElements());
        assertNotNull(enumeration.nextElement());

        //We should not be sending an advisory for dispatching to a browser
        Message msg = advisoryConsumer.receive(1000);
        assertNull(msg);
    }

    private void testMessageConsumerAdvisory(ActiveMQDestination dest, Function<ActiveMQDestination, Topic> advisoryTopicSupplier,
        boolean transacted) throws Exception {
        Session s = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = s.createConsumer(dest);
        assertNotNull(consumer);

        Topic advisoryTopic = advisoryTopicSupplier.apply(dest);
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        // start throwing messages at the consumer
        MessageProducer producer = s.createProducer(dest);

        BytesMessage m = s.createBytesMessage();
        m.writeBytes(new byte[1024]);
        producer.send(m);
        if (transacted) {
            s.commit();
        }

        Message msg = advisoryConsumer.receive(1000);
        assertNotNull(msg);
        if (transacted) {
            s.commit();
        }
        ActiveMQMessage message = (ActiveMQMessage) msg;
        ActiveMQMessage payload = (ActiveMQMessage) message.getDataStructure();

        //Could be either
        String originBrokerUrl = (String)message.getProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL);
        assertTrue(originBrokerUrl.startsWith("tcp://") || originBrokerUrl.startsWith("nio://"));
        assertEquals(message.getProperty(AdvisorySupport.MSG_PROPERTY_DESTINATION), dest.getQualifiedName());

        //Make sure consumer id exists if dispatched advisory
        if (AdvisorySupport.isMessageDispatchedAdvisoryTopic(advisoryTopic)) {
            assertNotNull(message.getStringProperty(AdvisorySupport.MSG_PROPERTY_CONSUMER_ID));
        }

        //Add assertion to make sure body is included for advisory topics
        //when includeBodyForAdvisory is true
        assertIncludeBodyForAdvisory(payload);
    }

    private void testDurableSubscriptionAdvisory(Function<ActiveMQDestination, Topic> advisoryTopicSupplier,
        boolean transacted) throws Exception {
        Session s = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
        Topic topic = s.createTopic(getClass().getName());
        MessageConsumer consumer = s.createDurableSubscriber(topic, "sub");
        assertNotNull(consumer);

        Topic advisoryTopic = advisoryTopicSupplier.apply((ActiveMQDestination) topic);
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        // start throwing messages at the consumer
        MessageProducer producer = s.createProducer(topic);

        BytesMessage m = s.createBytesMessage();
        m.writeBytes(new byte[1024]);
        producer.send(m);
        if (transacted) {
            s.commit();
        }

        Message msg = advisoryConsumer.receive(1000);
        assertNotNull(msg);
        if (transacted) {
            s.commit();
        }
        ActiveMQMessage message = (ActiveMQMessage) msg;
        ActiveMQMessage payload = (ActiveMQMessage) message.getDataStructure();

        //This should always be tcp:// because that is the transport that is used to connect even though
        //the nio transport is the first one in the list
        assertTrue(((String)message.getProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL)).startsWith("tcp://"));
        assertEquals(message.getProperty(AdvisorySupport.MSG_PROPERTY_DESTINATION), ((ActiveMQDestination) topic).getQualifiedName());

        //Add assertion to make sure body is included for advisory topics
        //when includeBodyForAdvisory is true
        assertIncludeBodyForAdvisory(payload);
    }

    @Test(timeout = 60000)
    public void testMessageConsumedAdvisory() throws Exception {
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = s.createQueue(getClass().getName());
        MessageConsumer consumer = s.createConsumer(queue);

        Topic advisoryTopic = AdvisorySupport.getMessageConsumedAdvisoryTopic((ActiveMQDestination) queue);
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        // start throwing messages at the consumer
        MessageProducer producer = s.createProducer(queue);

        BytesMessage m = s.createBytesMessage();
        m.writeBytes(new byte[1024]);
        producer.send(m);
        String id = m.getJMSMessageID();
        Message msg = consumer.receive(1000);
        assertNotNull(msg);

        msg = advisoryConsumer.receive(1000);
        assertNotNull(msg);

        ActiveMQMessage message = (ActiveMQMessage) msg;
        ActiveMQMessage payload = (ActiveMQMessage) message.getDataStructure();
        String originalId = payload.getJMSMessageID();
        assertEquals(originalId, id);

        //This should always be tcp:// because that is the transport that is used to connect even though
        //the nio transport is the first one in the list
        assertTrue(((String)message.getProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL)).startsWith("tcp://"));
        assertEquals(message.getProperty(AdvisorySupport.MSG_PROPERTY_DESTINATION), ((ActiveMQDestination) queue).getQualifiedName());

        //Add assertion to make sure body is included for advisory topics
        //when includeBodyForAdvisory is true
        assertIncludeBodyForAdvisory(payload);
    }

    @Test(timeout = 60000)
    public void testMessageExpiredAdvisoryQueueSubClient() throws Exception {
        testMessageExpiredAdvisoryQueue(new ActiveMQQueue(getClass().getName() + "client.timeout"),
            300000, true, 500);
    }

    @Test(timeout = 60000)
    public void testMessageExpiredAdvisoryQueueSubServer() throws Exception {
        testMessageExpiredAdvisoryQueue(new ActiveMQQueue(getClass().getName()), 1,true, 500);
    }

    @Test(timeout = 60000)
    public void testMessageExpiredAdvisoryQueueSubServerTask() throws Exception {
        testMessageExpiredAdvisoryQueue(new ActiveMQQueue(getClass().getName()), 1000,false,
            EXPIRE_MESSAGE_PERIOD * 2);
    }

    private void testMessageExpiredAdvisoryQueue(ActiveMQQueue dest, int ttl, boolean createConsumer, int receiveTimeout) throws Exception {
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);;

        Topic advisoryTopic = AdvisorySupport.getExpiredMessageTopic(dest);
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        // start throwing messages at the consumer
        MessageProducer producer = s.createProducer(dest);
        producer.setTimeToLive(ttl);

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(new byte[1024]);
            producer.send(m);
        }

        MessageConsumer consumer = null;
        if (createConsumer) {
            consumer = s.createConsumer(dest);
            assertNotNull(consumer);
        }

        Message msg = advisoryConsumer.receive(receiveTimeout);
        assertNotNull(msg);
        ActiveMQMessage message = (ActiveMQMessage) msg;
        ActiveMQMessage payload = (ActiveMQMessage) message.getDataStructure();

        //This should be set
        assertNotNull(message.getProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL));

        //Add assertion to make sure body is included for advisory topics
        //when includeBodyForAdvisory is true
        assertIncludeBodyForAdvisory(payload);
    }

    @Test(timeout = 60000)
    public void testMessageExpiredAdvisoryTopicSub() throws Exception {
        ActiveMQTopic dest = new ActiveMQTopic(getClass().getName());
        //Set prefetch to 1 so acks will trigger expiration on dispatching more messages
        broker.getDestinationPolicy().getDefaultEntry().setTopicPrefetch(1);
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);;

        MessageConsumer consumer =  s.createConsumer(dest);
        MessageConsumer expiredAdvisoryConsumer = s.createConsumer(AdvisorySupport.getExpiredMessageTopic(dest));
        MessageConsumer discardedAdvisoryConsumer = s.createConsumer(AdvisorySupport.getMessageDiscardedAdvisoryTopic(dest));

        // start throwing messages at the consumer
        MessageProducer producer = s.createProducer(dest);
        producer.setTimeToLive(10);
        for (int i = 0; i < 10; i++) {
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(new byte[1024]);
            producer.send(m);
        }
        Thread.sleep(500);

        //Receiving will trigger the server to discard on dispatch when acks are received
        //Currently the advisory is only fired on dispatch from server or messages added to ta topic
        //and not on expired acks from the client side as the original messages are not tracked in
        //dispatch list so the advisory can't be fired
        for (int i = 0; i < 10; i++) {
            assertNull(consumer.receive(10));
        }

        //Should no longer receive discard advisories for expiration
        assertNull(discardedAdvisoryConsumer.receive(1000));

        Message msg = expiredAdvisoryConsumer.receive(1000);
        assertNotNull(msg);
        ActiveMQMessage message = (ActiveMQMessage) msg;
        ActiveMQMessage payload = (ActiveMQMessage) message.getDataStructure();

        //This should be set
        assertNotNull(message.getProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL));

        //Add assertion to make sure body is included for advisory topics
        //when includeBodyForAdvisory is true
        assertIncludeBodyForAdvisory(payload);
    }

    @Test(timeout = 60000)
    public void testMessageExpiredAdvisoryDurableClient() throws Exception {
        testMessageExpiredDurableAdvisory(getClass().getName() + "client.timeout",
            300000, true, 500);
    }

    @Test(timeout = 60000)
    public void testMessageExpiredAdvisoryDurableServer() throws Exception {
        testMessageExpiredDurableAdvisory(getClass().getName(), 1,true, 500);
    }

    @Test(timeout = 60000)
    public void testMessageExpiredAdvisoryDurableServerTask() throws Exception {
        testMessageExpiredDurableAdvisory(getClass().getName(), 2000,false, EXPIRE_MESSAGE_PERIOD * 2);
    }

    private void testMessageExpiredDurableAdvisory(String topic, int ttl, boolean bringDurableOnline,
        int receiveTimeout) throws Exception {
        ActiveMQTopic dest = new ActiveMQTopic(topic);
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);;

        //create durable and send offline messages
        MessageConsumer consumer = s.createDurableSubscriber(dest, "sub1");
        consumer.close();

        Topic advisoryTopic = AdvisorySupport.getExpiredMessageTopic(dest);
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        // start throwing messages at the consumer
        MessageProducer producer = s.createProducer(dest);
        producer.setTimeToLive(ttl);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(new byte[1024]);
            producer.send(m);
        }

        //if flag is true then bring online to trigger expiration on dispatch
        if (bringDurableOnline) {
            consumer = s.createDurableSubscriber(dest, "sub1");
        }

        Message msg = advisoryConsumer.receive(receiveTimeout);
        assertNotNull(msg);
        ActiveMQMessage message = (ActiveMQMessage) msg;
        ActiveMQMessage payload = (ActiveMQMessage) message.getDataStructure();

        //This should be set
        assertNotNull(message.getProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL));

        //Add assertion to make sure body is included for advisory topics
        //when includeBodyForAdvisory is true
        assertIncludeBodyForAdvisory(payload);
    }

    @Test(timeout = 60000)
    public void testMessageDLQd() throws Exception {
        ActiveMQPrefetchPolicy policy = new ActiveMQPrefetchPolicy();
        policy.setTopicPrefetch(2);
        ((ActiveMQConnection) connection).setPrefetchPolicy(policy);
        Session s = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = s.createTopic(getClass().getName());

        Topic advisoryTopic = s.createTopic(">");
        for (int i = 0; i < 100; i++) {
            s.createConsumer(advisoryTopic);
        }
        MessageConsumer advisoryConsumer = s.createConsumer(AdvisorySupport.getMessageDLQdAdvisoryTopic((ActiveMQDestination) topic));

        MessageProducer producer = s.createProducer(topic);
        int count = 10;
        for (int i = 0; i < count; i++) {
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(new byte[1024]);
            producer.send(m);
        }

        Message msg = advisoryConsumer.receive(1000);
        assertNotNull(msg);
        ActiveMQMessage message = (ActiveMQMessage) msg;
        ActiveMQMessage payload = (ActiveMQMessage) message.getDataStructure();
        //This should be set
        assertNotNull(message.getProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL));
        //Add assertion to make sure body is included for DLQ advisory topics
        //when includeBodyForAdvisory is true
        assertIncludeBodyForAdvisory(payload);

        // we should get here without StackOverflow
    }

    @Test(timeout = 60000)
    public void testMessageDiscardedAdvisory() throws Exception {
        Session s = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = s.createTopic(getClass().getName());
        MessageConsumer consumer = s.createConsumer(topic);
        assertNotNull(consumer);

        Topic advisoryTopic = AdvisorySupport.getMessageDiscardedAdvisoryTopic((ActiveMQDestination) topic);
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        // start throwing messages at the consumer
        MessageProducer producer = s.createProducer(topic);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(new byte[1024]);
            producer.send(m);
        }

        Message msg = advisoryConsumer.receive(1000);
        assertNotNull(msg);
        ActiveMQMessage message = (ActiveMQMessage) msg;
        ActiveMQMessage payload = (ActiveMQMessage) message.getDataStructure();

        //This should be set
        assertNotNull(message.getProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL));
        assertEquals(message.getProperty(AdvisorySupport.MSG_PROPERTY_DESTINATION), ((ActiveMQDestination) topic).getQualifiedName());

        //Add assertion to make sure body is included for advisory topics
        //when includeBodyForAdvisory is true
        assertIncludeBodyForAdvisory(payload);
    }

    @Test(timeout = 60000)
    public void testMessageDeliveryVTAdvisory() throws Exception {
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQTopic vt = new ActiveMQTopic("VirtualTopic.TEST");

        ActiveMQQueue a  = new ActiveMQQueue("Consumer.A.VirtualTopic.TEST");
        MessageConsumer consumer = s.createConsumer(a);

        ActiveMQQueue b = new ActiveMQQueue("Consumer.B.VirtualTopic.TEST");
        MessageConsumer consumerB = s.createConsumer(b);

        assertNotNull(consumer);
        assertNotNull(consumerB);

        HashSet<String> dests = new HashSet<String>();
        dests.add(vt.getQualifiedName());
        dests.add(a.getQualifiedName());
        dests.add(b.getQualifiedName());


        Topic advisoryTopic = new ActiveMQTopic(AdvisorySupport.MESSAGE_DELIVERED_TOPIC_PREFIX + ">");
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);

        // throw messages at the vt
        MessageProducer producer = s.createProducer(vt);

        BytesMessage m = s.createBytesMessage();
        m.writeBytes(new byte[1024]);
        producer.send(m);

        Message msg = null;
        while ((msg = advisoryConsumer.receive(1000)) != null) {
            ActiveMQMessage message = (ActiveMQMessage) msg;
            String dest = (String) message.getProperty(AdvisorySupport.MSG_PROPERTY_DESTINATION);
            dests.remove(dest);
            assertIncludeBodyForAdvisory((ActiveMQMessage) message.getDataStructure());
        }

        assertTrue("Got delivered for all: " + dests, dests.isEmpty());
    }

    @Before
    public void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        ConnectionFactory factory = createConnectionFactory();
        connection = factory.createConnection();
        connection.setClientID("clientId");
        connection.start();
    }

    @After
    public void tearDown() throws Exception {
        try {
            connection.close();
        } catch (Exception e) {
            //swallow exception so we can still stop the broker even on error
        }
        if (broker != null) {
            broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(broker.getTransportConnectorByName("OpenWire").getPublishableConnectString());
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }

    protected void configureBroker(BrokerService answer) throws Exception {
        answer.setPersistent(persistent);
        answer.setDeleteAllMessagesOnStartup(true);

        PolicyEntry policy = new PolicyEntry();
        policy.setExpireMessagesPeriod(EXPIRE_MESSAGE_PERIOD);
        policy.setAdvisoryForFastProducers(true);
        policy.setAdvisoryForConsumed(true);
        policy.setAdvisoryForDelivery(true);
        policy.setAdvisoryForDispatched(true);
        policy.setAdvisoryForDiscardingMessages(true);
        policy.setAdvisoryForSlowConsumers(true);
        policy.setAdvisoryWhenFull(true);
        policy.setIncludeBodyForAdvisory(includeBodyForAdvisory);
        policy.setProducerFlowControl(false);
        policy.setDurableTopicPrefetch(DEFAULT_PREFETCH);
        policy.setTopicPrefetch(DEFAULT_PREFETCH);
        policy.setQueuePrefetch(DEFAULT_PREFETCH);

        ConstantPendingMessageLimitStrategy strategy = new ConstantPendingMessageLimitStrategy();
        strategy.setLimit(10);
        policy.setPendingMessageLimitStrategy(strategy);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        answer.setDestinationPolicy(pMap);
        answer.addConnector("nio://localhost:0");
        answer.addConnector("tcp://localhost:0").setName("OpenWire");
        answer.setDeleteAllMessagesOnStartup(true);

        // add a plugin to ensure the expiration happens on the client side rather
        // than broker side.
        answer.setPlugins(new BrokerPlugin[] { new BrokerPlugin() {

            @Override
            public Broker installPlugin(Broker broker) throws Exception {
                return new BrokerFilter(broker) {

                    @Override
                    public void preProcessDispatch(MessageDispatch messageDispatch) {
                        ActiveMQDestination dest = messageDispatch.getDestination();
                        if (dest != null && !AdvisorySupport.isAdvisoryTopic(dest) && messageDispatch.getDestination()
                            .getPhysicalName().contains("client.timeout")) {
                            // Set the expiration to now
                            messageDispatch.getMessage().setExpiration(System.currentTimeMillis() - 1000);
                        }

                        super.preProcessDispatch(messageDispatch);
                    }
                };
            }
        } });
    }

    protected void assertIncludeBodyForAdvisory(ActiveMQMessage payload) {
        if (includeBodyForAdvisory) {
            assertNotNull(payload.getContent());
        } else {
            assertNull(payload.getContent());
        }
    }
}
