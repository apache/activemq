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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.region.policy.RedeliveryPolicyMap;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.broker.util.RedeliveryPlugin;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Test;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReplicaRedeliveryPluginTest extends ReplicaPluginTestSupport {
    protected ActiveMQConnection firstBrokerConnection;
    protected ActiveMQConnection secondBrokerConnection;
    final long redeliveryDelayMillis = 2000;
    long initialRedeliveryDelayMillis = 4000;
    int maxBrokerRedeliveries = 2;
    @Override
    protected void setUp() throws Exception {
        firstBroker = createFirstBroker();
        secondBroker = createSecondBroker();
        firstBroker.setSchedulerSupport(true);
        secondBroker.setSchedulerSupport(true);
        destination = createDestination();

        RedeliveryPlugin redeliveryPlugin = new RedeliveryPlugin();
        RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
        redeliveryPolicy.setQueue(destination.getPhysicalName());
        redeliveryPolicy.setRedeliveryDelay(redeliveryDelayMillis);
        redeliveryPolicy.setInitialRedeliveryDelay(initialRedeliveryDelayMillis);
        redeliveryPolicy.setMaximumRedeliveries(maxBrokerRedeliveries);

        RedeliveryPolicy defaultPolicy = new RedeliveryPolicy();
        defaultPolicy.setRedeliveryDelay(1000);
        defaultPolicy.setInitialRedeliveryDelay(1000);
        defaultPolicy.setMaximumRedeliveries(0);

        RedeliveryPolicyMap redeliveryPolicyMap = new RedeliveryPolicyMap();
        redeliveryPolicyMap.setRedeliveryPolicyEntries(List.of(redeliveryPolicy));
        redeliveryPolicyMap.setDefaultEntry(defaultPolicy);
        redeliveryPlugin.setRedeliveryPolicyMap(redeliveryPolicyMap);
        redeliveryPlugin.setFallbackToDeadLetter(true);
        redeliveryPlugin.setSendToDlqIfMaxRetriesExceeded(true);

        BrokerPlugin firstBrokerReplicaPlugin = firstBroker.getPlugins()[0];
        firstBroker.setPlugins(new BrokerPlugin[]{redeliveryPlugin, firstBrokerReplicaPlugin});
        startFirstBroker();
        startSecondBroker();

        firstBrokerConnectionFactory = new ActiveMQConnectionFactory(firstBindAddress);
        secondBrokerConnectionFactory = new ActiveMQConnectionFactory(secondBindAddress);

        firstBrokerConnection = (ActiveMQConnection) firstBrokerConnectionFactory.createConnection();
        firstBrokerConnection.start();

        secondBrokerConnection = (ActiveMQConnection) secondBrokerConnectionFactory.createConnection();
        secondBrokerConnection.start();

        waitUntilReplicationQueueHasConsumer(firstBroker);
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

        super.tearDown();
    }

    @Test
    public void testMessageRedelivery() throws Exception {
        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(destination);
        RedeliveryPolicy firstBrokerRedeliveryPolicy = new RedeliveryPolicy();
        firstBrokerRedeliveryPolicy.setInitialRedeliveryDelay(0);
        firstBrokerRedeliveryPolicy.setMaximumRedeliveries(0);
        ActiveMQConnection firstBrokerConsumerConnection = (ActiveMQConnection) firstBrokerConnectionFactory.createConnection();
        firstBrokerConsumerConnection.setRedeliveryPolicy(firstBrokerRedeliveryPolicy);
        firstBrokerConsumerConnection.start();
        Session producerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);

        Message message = producerSession.createMessage();
        message.setStringProperty("data", getName());
        producer.send(message);

        Session consumerSession = firstBrokerConsumerConnection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer firstBrokerConsumer = consumerSession.createConsumer(destination);

        Message secondBrokerReceivedMsg = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull("second broker got message", secondBrokerReceivedMsg);

        Message receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull("got message", receivedMessage);
        consumerSession.rollback();

        for (int i=0; i < maxBrokerRedeliveries; i++) {
            Message shouldBeNullMessage = firstBrokerConsumer.receive(redeliveryDelayMillis / 4);
            assertNull(shouldBeNullMessage);
            shouldBeNullMessage = secondBrokerConsumer.receive(redeliveryDelayMillis / 4);
            assertNull(shouldBeNullMessage);
            TimeUnit.SECONDS.sleep(4);

            Message brokerRedeliveryMessage = firstBrokerConsumer.receive(1500);
            assertNotNull("got message via broker redelivery after delay", brokerRedeliveryMessage);
            assertEquals("message matches", message.getStringProperty("data"), brokerRedeliveryMessage.getStringProperty("data"));
            System.out.println("received message: " + brokerRedeliveryMessage);
            assertEquals("has expiryDelay specified - iteration:" + i, i == 0 ? initialRedeliveryDelayMillis : redeliveryDelayMillis, brokerRedeliveryMessage.getLongProperty(RedeliveryPlugin.REDELIVERY_DELAY));

            brokerRedeliveryMessage = secondBrokerConsumer.receive(LONG_TIMEOUT);
            assertNull("should not receive message", brokerRedeliveryMessage);

            consumerSession.rollback();
        }

        producerSession.close();
        secondBrokerSession.close();
        firstBrokerConsumerConnection.close();
    }

    @Test
    public void testMessageDeliveredToDlq() throws Exception {
        ActiveMQDestination testDestination = new ActiveMQQueue(getName());
        Session secondBrokerSession = secondBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer secondBrokerConsumer = secondBrokerSession.createConsumer(testDestination);
        RedeliveryPolicy firstBrokerRedeliveryPolicy = new RedeliveryPolicy();
        firstBrokerRedeliveryPolicy.setInitialRedeliveryDelay(0);
        firstBrokerRedeliveryPolicy.setMaximumRedeliveries(0);
        ActiveMQConnection firstBrokerConsumerConnection = (ActiveMQConnection) firstBrokerConnectionFactory.createConnection();
        firstBrokerConsumerConnection.setRedeliveryPolicy(firstBrokerRedeliveryPolicy);
        firstBrokerConsumerConnection.start();
        Session producerSession = firstBrokerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(testDestination);

        Message message = producerSession.createMessage();
        message.setStringProperty("data", getName());
        producer.send(message);

        Session consumerSession = firstBrokerConsumerConnection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer firstBrokerConsumer = consumerSession.createConsumer(testDestination);

        Message secondBrokerReceivedMsg = secondBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull("second broker got message", secondBrokerReceivedMsg);

        Message receivedMessage = firstBrokerConsumer.receive(LONG_TIMEOUT);
        assertNotNull("got message", receivedMessage);
        consumerSession.rollback();

        MessageConsumer firstDlqConsumer = consumerSession.createConsumer(new ActiveMQQueue(SharedDeadLetterStrategy.DEFAULT_DEAD_LETTER_QUEUE_NAME));
        Message dlqMessage = firstDlqConsumer.receive(SHORT_TIMEOUT);
        assertNotNull("Got message from dql", dlqMessage);
        assertEquals("message matches", message.getStringProperty("data"), dlqMessage.getStringProperty("data"));

        MessageConsumer secondDlqConsumer = secondBrokerSession.createConsumer(new ActiveMQQueue(SharedDeadLetterStrategy.DEFAULT_DEAD_LETTER_QUEUE_NAME));
        dlqMessage = secondDlqConsumer.receive(LONG_TIMEOUT);
        assertNotNull("Got message from dql", dlqMessage);
        assertEquals("message matches", message.getStringProperty("data"), dlqMessage.getStringProperty("data"));
        consumerSession.commit();

        producerSession.close();
        secondBrokerSession.close();
        consumerSession.close();
        firstBrokerConsumerConnection.close();
    }
}
