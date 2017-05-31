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
package org.apache.activemq.broker;

import java.util.concurrent.TimeUnit;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.region.policy.RedeliveryPolicyMap;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.broker.util.RedeliveryPlugin;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerRedeliveryTest extends org.apache.activemq.TestSupport {

    static final Logger LOG = LoggerFactory.getLogger(BrokerRedeliveryTest.class);
    BrokerService broker = null;

    final ActiveMQQueue destination = new ActiveMQQueue("Redelivery");
    final String data = "hi";
    final long redeliveryDelayMillis = 2000;
    long initialRedeliveryDelayMillis = 4000;
    int maxBrokerRedeliveries = 2;

    public void testScheduledRedelivery() throws Exception {
        doTestScheduledRedelivery(maxBrokerRedeliveries, true);
    }

    public void testInfiniteRedelivery() throws Exception {
        initialRedeliveryDelayMillis = redeliveryDelayMillis;
        maxBrokerRedeliveries = RedeliveryPolicy.NO_MAXIMUM_REDELIVERIES;
        doTestScheduledRedelivery(RedeliveryPolicy.DEFAULT_MAXIMUM_REDELIVERIES + 1, false);
    }

    public void doTestScheduledRedelivery(int maxBrokerRedeliveriesToValidate, boolean validateDLQ) throws Exception {

        startBroker(true);
        sendMessage(0);

        ActiveMQConnection consumerConnection = (ActiveMQConnection) createConnection();
        RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
        redeliveryPolicy.setInitialRedeliveryDelay(0);
        redeliveryPolicy.setMaximumRedeliveries(0);
        consumerConnection.setRedeliveryPolicy(redeliveryPolicy);
        consumerConnection.start();
        Session consumerSession = consumerConnection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = consumerSession.createConsumer(destination);
        Message message = consumer.receive(1000);
        assertNotNull("got message", message);
        LOG.info("got: " + message);
        consumerSession.rollback();

        for (int i = 0; i < maxBrokerRedeliveriesToValidate; i++) {
            Message shouldBeNull = consumer.receive(500);
            assertNull("did not get message early: " + shouldBeNull, shouldBeNull);

            TimeUnit.SECONDS.sleep(4);

            Message brokerRedeliveryMessage = consumer.receive(1500);
            LOG.info("got: " + brokerRedeliveryMessage);
            assertNotNull("got message via broker redelivery after delay", brokerRedeliveryMessage);
            assertEquals("message matches", message.getStringProperty("data"), brokerRedeliveryMessage.getStringProperty("data"));
            assertEquals("has expiryDelay specified - iteration:" + i, i == 0 ? initialRedeliveryDelayMillis : redeliveryDelayMillis, brokerRedeliveryMessage.getLongProperty(RedeliveryPlugin.REDELIVERY_DELAY));

            consumerSession.rollback();
        }

        if (validateDLQ) {
            MessageConsumer dlqConsumer = consumerSession.createConsumer(new ActiveMQQueue(SharedDeadLetterStrategy.DEFAULT_DEAD_LETTER_QUEUE_NAME));
            Message dlqMessage = dlqConsumer.receive(2000);
            assertNotNull("Got message from dql", dlqMessage);
            assertEquals("message matches", message.getStringProperty("data"), dlqMessage.getStringProperty("data"));
            consumerSession.commit();
        } else {
            // consume/commit ok
            message = consumer.receive(3000);
            assertNotNull("got message", message);
            assertEquals("redeliveries accounted for", maxBrokerRedeliveriesToValidate + 2, message.getLongProperty("JMSXDeliveryCount"));
            consumerSession.commit();
        }

        consumerConnection.close();
    }

    public void testNoScheduledRedeliveryOfExpired() throws Exception {
        startBroker(true);
        ActiveMQConnection consumerConnection = (ActiveMQConnection) createConnection();
        consumerConnection.start();
        Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(destination);
        sendMessage(1500);
        Message message = consumer.receive(1000);
        assertNotNull("got message", message);

        // ensure there is another consumer to redispatch to
        MessageConsumer redeliverConsumer = consumerSession.createConsumer(destination);

        // allow consumed to expire so it gets redelivered
        TimeUnit.SECONDS.sleep(2);
        consumer.close();

        // should go to dlq as it has expired
        // validate DLQ
        MessageConsumer dlqConsumer = consumerSession.createConsumer(new ActiveMQQueue(SharedDeadLetterStrategy.DEFAULT_DEAD_LETTER_QUEUE_NAME));
        Message dlqMessage = dlqConsumer.receive(2000);
        assertNotNull("Got message from dql", dlqMessage);
        assertEquals("message matches", message.getStringProperty("data"), dlqMessage.getStringProperty("data"));
    }

    private void sendMessage(int timeToLive) throws Exception {
        ActiveMQConnection producerConnection = (ActiveMQConnection) createConnection();
        producerConnection.start();
        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        if (timeToLive > 0) {
            producer.setTimeToLive(timeToLive);
        }
        Message message = producerSession.createMessage();
        message.setStringProperty("data", data);
        producer.send(message);
        producerConnection.close();
    }

    private void startBroker(boolean deleteMessages) throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setSchedulerSupport(true);

        RedeliveryPlugin redeliveryPlugin = new RedeliveryPlugin();

        RedeliveryPolicy brokerRedeliveryPolicy = new RedeliveryPolicy();
        brokerRedeliveryPolicy.setRedeliveryDelay(redeliveryDelayMillis);
        brokerRedeliveryPolicy.setInitialRedeliveryDelay(initialRedeliveryDelayMillis);
        brokerRedeliveryPolicy.setMaximumRedeliveries(maxBrokerRedeliveries);

        RedeliveryPolicyMap redeliveryPolicyMap = new RedeliveryPolicyMap();
        redeliveryPolicyMap.setDefaultEntry(brokerRedeliveryPolicy);
        redeliveryPlugin.setRedeliveryPolicyMap(redeliveryPolicyMap);

        broker.setPlugins(new BrokerPlugin[]{redeliveryPlugin});

        if (deleteMessages) {
            broker.setDeleteAllMessagesOnStartup(true);
        }
        broker.start();
    }

    private void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker = null;
        }
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost");
    }

    @Override
    protected void tearDown() throws Exception {
        stopBroker();
        super.tearDown();
    }
}
