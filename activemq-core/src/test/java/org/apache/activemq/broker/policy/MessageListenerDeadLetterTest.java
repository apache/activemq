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
package org.apache.activemq.broker.policy;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MessageListenerDeadLetterTest extends DeadLetterTestSupport {
    private static final Log LOG = LogFactory
            .getLog(MessageListenerDeadLetterTest.class);

    private int rollbackCount;

    private Session dlqSession;

    private final Error[] error = new Error[1];

    protected void doTest() throws Exception {
        messageCount = 200;
        connection.start();

        ActiveMQConnection amqConnection = (ActiveMQConnection) connection;
        rollbackCount = amqConnection.getRedeliveryPolicy().getMaximumRedeliveries() + 1;
        LOG.info("Will redeliver messages: " + rollbackCount + " times");

        makeConsumer();
        makeDlqConsumer();

        sendMessages();

        // now lets receive and rollback N times
        int maxRollbacks = messageCount * rollbackCount;
        consumer.setMessageListener(new RollbackMessageListener(maxRollbacks, rollbackCount));

        for (int i = 0; i < messageCount; i++) {
            Message msg = dlqConsumer.receive(4000);
            if (error[0] != null) {
                // error from message listener
                throw error[0];
            }
            assertMessage(msg, i);
            assertNotNull("Should be a DLQ message for loop: " + i, msg);
        }
        if (error[0] != null) {
            throw error[0];
        }
    }

    protected void makeDlqConsumer() throws JMSException {
        dlqDestination = createDlqDestination();

        LOG.info("Consuming from dead letter on: " + dlqDestination);
        dlqConsumer = dlqSession.createConsumer(dlqDestination);
    }

    @Override
    protected void setUp() throws Exception {
        transactedMode = true;
        super.setUp();
        dlqSession = connection.createSession(transactedMode, acknowledgeMode);
    }

    @Override
    protected void tearDown() throws Exception {
        dlqConsumer.close();
        dlqSession.close();
        session.close();
        super.tearDown();
    };

    protected ActiveMQConnectionFactory createConnectionFactory()
            throws Exception {
        ActiveMQConnectionFactory answer = super.createConnectionFactory();
        RedeliveryPolicy policy = new RedeliveryPolicy();
        policy.setMaximumRedeliveries(3);
        policy.setBackOffMultiplier((short) 1);
        policy.setInitialRedeliveryDelay(0);
        policy.setUseExponentialBackOff(false);
        answer.setRedeliveryPolicy(policy);
        return answer;
    }

    protected Destination createDlqDestination() {
        return new ActiveMQQueue("ActiveMQ.DLQ");
    }

    class RollbackMessageListener implements MessageListener {

        final int maxRollbacks;

        final int deliveryCount;

        AtomicInteger rollbacks = new AtomicInteger();

        RollbackMessageListener(int c, int delvery) {
            maxRollbacks = c;
            deliveryCount = delvery;
        }

        public void onMessage(Message message) {
            try {
                int expectedMessageId = rollbacks.get() / deliveryCount;
                LOG.info("expecting messageId: " + expectedMessageId);
                assertMessage(message, expectedMessageId);
                if (rollbacks.incrementAndGet() > maxRollbacks) {
                    fail("received too many messages, already done too many rollbacks: "
                            + rollbacks);
                }
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