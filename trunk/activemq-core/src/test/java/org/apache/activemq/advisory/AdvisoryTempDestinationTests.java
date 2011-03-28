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

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.ConstantPendingMessageLimitStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;

public class AdvisoryTempDestinationTests extends TestCase {

    protected static final int MESSAGE_COUNT = 2000;
    protected BrokerService broker;
    protected Connection connection;
    protected String bindAddress = ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL;
    protected int topicCount;


    public void testNoSlowConsumerAdvisory() throws Exception {
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = s.createTemporaryQueue();
        MessageConsumer consumer = s.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
            }
        });
        Topic advisoryTopic = AdvisorySupport
                .getSlowConsumerAdvisoryTopic((ActiveMQDestination) queue);
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

    public void testSlowConsumerAdvisory() throws Exception {
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = s.createTemporaryQueue();
        MessageConsumer consumer = s.createConsumer(queue);

        Topic advisoryTopic = AdvisorySupport
                .getSlowConsumerAdvisoryTopic((ActiveMQDestination) queue);
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
        assertNotNull(msg);
    }

    public void testMessageDeliveryAdvisory() throws Exception {
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = s.createTemporaryQueue();
        MessageConsumer consumer = s.createConsumer(queue);

        Topic advisoryTopic = AdvisorySupport.getMessageDeliveredAdvisoryTopic((ActiveMQDestination) queue);
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        //start throwing messages at the consumer
        MessageProducer producer = s.createProducer(queue);

        BytesMessage m = s.createBytesMessage();
        m.writeBytes(new byte[1024]);
        producer.send(m);

        Message msg = advisoryConsumer.receive(1000);
        assertNotNull(msg);
    }

    public void testTempMessageConsumedAdvisory() throws Exception {
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue queue = s.createTemporaryQueue();
        MessageConsumer consumer = s.createConsumer(queue);

        Topic advisoryTopic = AdvisorySupport.getMessageConsumedAdvisoryTopic((ActiveMQDestination) queue);
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        //start throwing messages at the consumer
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
    }

    public void testMessageExpiredAdvisory() throws Exception {
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = s.createQueue(getClass().getName());
        MessageConsumer consumer = s.createConsumer(queue);

        Topic advisoryTopic = AdvisorySupport.getExpiredMessageTopic((ActiveMQDestination) queue);
        MessageConsumer advisoryConsumer = s.createConsumer(advisoryTopic);
        //start throwing messages at the consumer
        MessageProducer producer = s.createProducer(queue);
        producer.setTimeToLive(1);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            BytesMessage m = s.createBytesMessage();
            m.writeBytes(new byte[1024]);
            producer.send(m);
        }

        Message msg = advisoryConsumer.receive(2000);
        assertNotNull(msg);
    }

    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        ConnectionFactory factory = createConnectionFactory();
        connection = factory.createConnection();
        connection.start();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        connection.close();
        if (broker != null) {
            broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory()
            throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
                ActiveMQConnection.DEFAULT_BROKER_URL);
        return cf;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }

    protected void configureBroker(BrokerService answer) throws Exception {
        answer.setPersistent(false);
        ConstantPendingMessageLimitStrategy strategy = new ConstantPendingMessageLimitStrategy();
        strategy.setLimit(10);
        PolicyEntry tempQueueEntry = createPolicyEntry(strategy);
        tempQueueEntry.setTempQueue(true);
        PolicyEntry tempTopicEntry = createPolicyEntry(strategy);
        tempTopicEntry.setTempTopic(true);

        PolicyMap pMap = new PolicyMap();
        final List<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();
        policyEntries.add(tempQueueEntry);
        policyEntries.add(tempTopicEntry);
        pMap.setPolicyEntries(policyEntries);

        answer.setDestinationPolicy(pMap);
        answer.addConnector(bindAddress);
        answer.setDeleteAllMessagesOnStartup(true);
    }

    private PolicyEntry createPolicyEntry(ConstantPendingMessageLimitStrategy strategy) {
        PolicyEntry policy = new PolicyEntry();
        policy.setAdvisdoryForFastProducers(true);
        policy.setAdvisoryForConsumed(true);
        policy.setAdvisoryForDelivery(true);
        policy.setAdvisoryForDiscardingMessages(true);
        policy.setAdvisoryForSlowConsumers(true);
        policy.setAdvisoryWhenFull(true);
        policy.setProducerFlowControl(false);
        policy.setPendingMessageLimitStrategy(strategy);

        return policy;
    }
}
