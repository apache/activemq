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
package org.apache.activemq.bugs;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ3405Test extends TestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ3405Test.class);

    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private MessageProducer producer;
    private int deliveryMode = DeliveryMode.PERSISTENT;
    private Destination dlqDestination;
    private MessageConsumer dlqConsumer;
    private BrokerService broker;

    private int messageCount;
    private Destination destination;
    private int rollbackCount;
    private Session dlqSession;
    private final Error[] error = new Error[1];
    private boolean topic = true;
    private boolean durableSubscriber = true;

    public void testTransientTopicMessage() throws Exception {
        topic = true;
        deliveryMode = DeliveryMode.NON_PERSISTENT;
        durableSubscriber = true;
        doTest();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        PolicyEntry policy = new PolicyEntry();
        DeadLetterStrategy defaultDeadLetterStrategy = policy.getDeadLetterStrategy();
        if(defaultDeadLetterStrategy!=null) {
            defaultDeadLetterStrategy.setProcessNonPersistent(true);
        }
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(pMap);
        return broker;
    }

    protected void doTest() throws Exception {
        messageCount = 200;
        connection.start();

        final QueueViewMBean dlqView = getProxyToDLQ();

        ActiveMQConnection amqConnection = (ActiveMQConnection) connection;
        rollbackCount = amqConnection.getRedeliveryPolicy().getMaximumRedeliveries() + 1;
        LOG.info("Will redeliver messages: " + rollbackCount + " times");

        makeConsumer();
        makeDlqConsumer();
        dlqConsumer.close();

        sendMessages();

        // now lets receive and rollback N times
        int maxRollbacks = messageCount * rollbackCount;

        consumer.setMessageListener(new RollbackMessageListener(maxRollbacks, rollbackCount));

        // We receive and rollback into the DLQ N times moving the DLQ messages back to their
        // original Q to test that they are continually placed back in the DLQ.
        for (int i = 0; i < 2; ++i) {

            assertTrue("DLQ was not filled as expected", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return dlqView.getQueueSize() == messageCount;
                }
            }));

            connection.stop();

            assertEquals("DLQ should be full now.", messageCount, dlqView.getQueueSize());

            String moveTo;
            if (topic) {
                moveTo = "topic://" + ((Topic) getDestination()).getTopicName();
            } else {
                moveTo = "queue://" + ((Queue) getDestination()).getQueueName();
            }

            LOG.debug("Moving " + messageCount + " messages from ActiveMQ.DLQ to " + moveTo);
            dlqView.moveMatchingMessagesTo("", moveTo);

            assertTrue("DLQ was not emptied as expected", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return dlqView.getQueueSize() == 0;
                }
            }));

            connection.start();
        }
    }

    protected void makeConsumer() throws JMSException {
        Destination destination = getDestination();
        LOG.info("Consuming from: " + destination);
        if (durableSubscriber) {
            consumer = session.createDurableSubscriber((Topic)destination, destination.toString());
        } else {
            consumer = session.createConsumer(destination);
        }
    }

    protected void makeDlqConsumer() throws JMSException {
        dlqDestination = createDlqDestination();

        LOG.info("Consuming from dead letter on: " + dlqDestination);
        dlqConsumer = dlqSession.createConsumer(dlqDestination);
    }

    @Override
    protected void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();

        connection = createConnection();
        connection.setClientID(createClientId());

        session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        connection.start();

        dlqSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    protected void tearDown() throws Exception {
        dlqConsumer.close();
        dlqSession.close();
        session.close();

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    };

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory()
            throws Exception {
        ActiveMQConnectionFactory answer = super.createConnectionFactory();
        RedeliveryPolicy policy = new RedeliveryPolicy();
        policy.setMaximumRedeliveries(3);
        policy.setBackOffMultiplier((short) 1);
        policy.setRedeliveryDelay(0);
        policy.setInitialRedeliveryDelay(0);
        policy.setUseExponentialBackOff(false);
        answer.setRedeliveryPolicy(policy);
        return answer;
    }

    protected void sendMessages() throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(getDestination());
        producer.setDeliveryMode(deliveryMode);

        LOG.info("Sending " + messageCount + " messages to: " + getDestination());
        for (int i = 0; i < messageCount; i++) {
            Message message = createMessage(session, i);
            producer.send(message);
        }
    }

    protected TextMessage createMessage(Session session, int i) throws JMSException {
        return session.createTextMessage(getMessageText(i));
    }

    protected String getMessageText(int i) {
        return "message: " + i;
    }

    protected Destination createDlqDestination() {
        return new ActiveMQQueue("ActiveMQ.DLQ");
    }

    private QueueViewMBean getProxyToDLQ() throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=localhost," +
            "destinationType=Queue,destinationName=ActiveMQ.DLQ");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected Destination getDestination() {
        if (destination == null) {
            destination = createDestination();
        }
        return destination;
    }

    protected String createClientId() {
        return toString();
    }

    class RollbackMessageListener implements MessageListener {

        final int maxRollbacks;
        final int deliveryCount;
        final AtomicInteger rollbacks = new AtomicInteger();

        RollbackMessageListener(int c, int delvery) {
            maxRollbacks = c;
            deliveryCount = delvery;
        }

        @Override
        public void onMessage(Message message) {
            try {
                int expectedMessageId = rollbacks.get() / deliveryCount;
                LOG.info("expecting messageId: " + expectedMessageId);
                rollbacks.incrementAndGet();
                session.rollback();
            } catch (Throwable e) {
                LOG.error("unexpected exception:" + e, e);
                // propagating assertError to execution task will cause a hang
                // at shutdown
                if (e instanceof Error) {
                    error[0] = (Error) e;
                } else {
                    fail("unexpected exception: " + e);
                }
            }
        }
    }
}
