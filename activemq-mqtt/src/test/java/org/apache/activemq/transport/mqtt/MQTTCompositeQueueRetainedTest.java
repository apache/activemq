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
package org.apache.activemq.transport.mqtt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.policy.RetainedMessageSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.virtual.CompositeTopic;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.ByteSequence;
import org.junit.Test;

/**
 *
 */
public class MQTTCompositeQueueRetainedTest extends MQTTTestSupport {

    // configure composite topic
    private static final String COMPOSITE_TOPIC = "Composite.TopicA";
    private static final String FORWARD_QUEUE = "Composite.Queue.A";
    private static final String FORWARD_TOPIC = "Composite.Topic.A";

    private static final int NUM_MESSAGES = 25;

    @Override
    protected BrokerService createBroker(boolean deleteAllOnStart) throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(isPersistent());
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(isSchedulerSupportEnabled());
        brokerService.setPopulateJMSXUserID(true);
        brokerService.setUseJmx(false);

        final CompositeTopic compositeTopic = new CompositeTopic();
        compositeTopic.setName(COMPOSITE_TOPIC);
        final ArrayList<ActiveMQDestination> forwardDestinations = new ArrayList<ActiveMQDestination>();
        forwardDestinations.add(new ActiveMQQueue(FORWARD_QUEUE));
        forwardDestinations.add(new ActiveMQTopic(FORWARD_TOPIC));
        compositeTopic.setForwardTo(forwardDestinations);
        // NOTE: allows retained messages to be set on the Composite
        compositeTopic.setForwardOnly(false);

        final VirtualDestinationInterceptor destinationInterceptor = new VirtualDestinationInterceptor();
        destinationInterceptor.setVirtualDestinations(new VirtualDestination[] {compositeTopic} );
        brokerService.setDestinationInterceptors(new DestinationInterceptor[] { destinationInterceptor });

        return brokerService;
    }

    @Test(timeout = 60 * 1000)
    public void testSendMQTTReceiveJMSCompositeDestinations() throws Exception {

        final MQTTClientProvider provider = getMQTTClientProvider();
        initializeConnection(provider);

        // send retained message
        final String MQTT_TOPIC = "Composite/TopicA";
        final String RETAINED = "RETAINED";
        provider.publish(MQTT_TOPIC, RETAINED.getBytes(), AT_LEAST_ONCE, true);

        ActiveMQConnection activeMQConnection = (ActiveMQConnection) new ActiveMQConnectionFactory(jmsUri).createConnection();
        // MUST set to true to receive retained messages
        activeMQConnection.setUseRetroactiveConsumer(true);
        activeMQConnection.setClientID("jms-client");
        activeMQConnection.start();
        Session s = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        javax.jms.Queue jmsQueue = s.createQueue(FORWARD_QUEUE);
        javax.jms.Topic jmsTopic = s.createTopic(FORWARD_TOPIC);

        MessageConsumer queueConsumer = s.createConsumer(jmsQueue);
        MessageConsumer topicConsumer = s.createDurableSubscriber(jmsTopic, "jms-subscription");

        // check whether we received retained message twice on mapped Queue, once marked as RETAINED
        ActiveMQMessage message;
        ByteSequence bs;
        for (int i = 0; i < 2; i++) {
            message = (ActiveMQMessage) queueConsumer.receive(5000);
            assertNotNull("Should get retained message from " + FORWARD_QUEUE, message);
            bs = message.getContent();
            assertEquals(RETAINED, new String(bs.data, bs.offset, bs.length));
            assertTrue(message.getBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAIN_PROPERTY) != message.getBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAINED_PROPERTY));
        }

        // check whether we received retained message on mapped Topic
        message = (ActiveMQMessage) topicConsumer.receive(5000);
        assertNotNull("Should get retained message from " + FORWARD_TOPIC, message);
        bs = message.getContent();
        assertEquals(RETAINED, new String(bs.data, bs.offset, bs.length));
        assertFalse(message.getBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAIN_PROPERTY));
        assertTrue(message.getBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAINED_PROPERTY));

        for (int i = 0; i < NUM_MESSAGES; i++) {
            String payload = "Test Message: " + i;
            provider.publish(MQTT_TOPIC, payload.getBytes(), AT_LEAST_ONCE);

            message = (ActiveMQMessage) queueConsumer.receive(5000);
            assertNotNull("Should get a message from " + FORWARD_QUEUE, message);
            bs = message.getContent();
            assertEquals(payload, new String(bs.data, bs.offset, bs.length));

            message = (ActiveMQMessage) topicConsumer.receive(5000);
            assertNotNull("Should get a message from " + FORWARD_TOPIC, message);
            bs = message.getContent();
            assertEquals(payload, new String(bs.data, bs.offset, bs.length));
        }

        // close consumer and look for retained messages again
        queueConsumer.close();
        topicConsumer.close();

        queueConsumer = s.createConsumer(jmsQueue);
        topicConsumer = s.createDurableSubscriber(jmsTopic, "jms-subscription");

        // check whether we received retained message on mapped Queue, again
        message = (ActiveMQMessage) queueConsumer.receive(5000);
        assertNotNull("Should get recovered retained message from " + FORWARD_QUEUE, message);
        bs = message.getContent();
        assertEquals(RETAINED, new String(bs.data, bs.offset, bs.length));
        assertTrue(message.getBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAINED_PROPERTY));
        assertNull("Should not get second retained message from " + FORWARD_QUEUE, queueConsumer.receive(2000));

        // check whether we received retained message on mapped Topic, again
        message = (ActiveMQMessage) topicConsumer.receive(5000);
        assertNotNull("Should get recovered retained message from " + FORWARD_TOPIC, message);
        bs = message.getContent();
        assertEquals(RETAINED, new String(bs.data, bs.offset, bs.length));
        assertTrue(message.getBooleanProperty(RetainedMessageSubscriptionRecoveryPolicy.RETAINED_PROPERTY));
        assertNull("Should not get second retained message from " + FORWARD_TOPIC, topicConsumer.receive(2000));

        // create second queue consumer and verify that it doesn't trigger message recovery
        final MessageConsumer queueConsumer2 = s.createConsumer(jmsQueue);
        assertNull("Second consumer MUST not receive retained message from " + FORWARD_QUEUE, queueConsumer2.receive(2000));

        activeMQConnection.close();
        provider.disconnect();
    }
}
