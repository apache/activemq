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

import junit.framework.Test;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.RedeliveryPolicyMap;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.broker.util.RedeliveryPlugin;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerRedeliveryTest extends org.apache.activemq.TestSupport {

    static final Logger LOG = LoggerFactory.getLogger(BrokerRedeliveryTest.class);
    BrokerService broker = null;
    TransportConnector tcpConnector = null;

    final ActiveMQQueue destination = new ActiveMQQueue("Redelivery");
    final String data = "hi";
    final long redeliveryDelayMillis = 2000;
    long initialRedeliveryDelayMillis = 4000;
    int maxBrokerRedeliveries = 2;
    public Boolean checkForDuplicates = Boolean.TRUE;

    public void initCombosForTestScheduledRedelivery() {
        addCombinationValues("checkForDuplicates", new Object[] {Boolean.TRUE, Boolean.FALSE});
    }

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

        consumerConnection.close();
    }

    public void testNoScheduledRedeliveryOfDuplicates() throws Exception {
        broker = createBroker(true);

        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setUseCache(false); // disable the cache such that duplicates are not suppressed on send

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.start();

        ActiveMQConnection consumerConnection = (ActiveMQConnection) createConnection();
        consumerConnection.start();
        Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(destination);

        ActiveMQConnection producerConnection = (ActiveMQConnection) createConnection();
        producerConnection.start();
        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(destination);
        Message message = producerSession.createMessage();
        message.setStringProperty("data", data);
        producer.send(message);

        message = consumer.receive(1000);
        assertNotNull("got message", message);
        message.acknowledge();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                // wait for ack to be processes
                LOG.info("Total message count: " + broker.getAdminView().getTotalMessageCount());
                return broker.getAdminView().getTotalMessageCount() == 0;
            }
        });
        // send it again
        // should go to dlq as a duplicate from the store
        producerConnection.getTransport().request(message);

        // validate DLQ
        MessageConsumer dlqConsumer = consumerSession.createConsumer(new ActiveMQQueue(SharedDeadLetterStrategy.DEFAULT_DEAD_LETTER_QUEUE_NAME));
        Message dlqMessage = dlqConsumer.receive(4000);
        assertNotNull("Got message from dql", dlqMessage);
        assertEquals("message matches", message.getStringProperty("data"), dlqMessage.getStringProperty("data"));

        consumerConnection.close();
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
        broker = createBroker(false);
        if (deleteMessages) {
            broker.setDeleteAllMessagesOnStartup(true);
        }
        broker.start();
    }

    private BrokerService createBroker(boolean persistent) throws Exception {
        broker = new BrokerService();
        broker.setPersistent(persistent);
        broker.setSchedulerSupport(true);
        tcpConnector = broker.addConnector("tcp://localhost:0");

        RedeliveryPlugin redeliveryPlugin = new RedeliveryPlugin();

        RedeliveryPolicy brokerRedeliveryPolicy = new RedeliveryPolicy();
        brokerRedeliveryPolicy.setRedeliveryDelay(redeliveryDelayMillis);
        brokerRedeliveryPolicy.setInitialRedeliveryDelay(initialRedeliveryDelayMillis);
        brokerRedeliveryPolicy.setMaximumRedeliveries(maxBrokerRedeliveries);

        RedeliveryPolicyMap redeliveryPolicyMap = new RedeliveryPolicyMap();
        redeliveryPolicyMap.setDefaultEntry(brokerRedeliveryPolicy);
        redeliveryPlugin.setRedeliveryPolicyMap(redeliveryPolicyMap);

        broker.setPlugins(new BrokerPlugin[]{redeliveryPlugin});
        return broker;
    }

    private void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker = null;
        }
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("failover:(" + tcpConnector.getPublishableConnectString() + ")?jms.checkForDuplicates=" + checkForDuplicates.toString());
    }

    @Override
    protected void tearDown() throws Exception {
        stopBroker();
        super.tearDown();
    }

    public static Test suite() {
        return suite(BrokerRedeliveryTest.class);
    }
}
