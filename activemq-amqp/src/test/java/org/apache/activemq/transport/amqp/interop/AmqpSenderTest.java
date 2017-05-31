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
package org.apache.activemq.transport.amqp.interop;

import static org.apache.activemq.transport.amqp.AmqpSupport.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.transport.amqp.client.AmqpSupport;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.activemq.util.Wait;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.junit.Test;

/**
 * Test broker behavior when creating AMQP senders
 */
public class AmqpSenderTest extends AmqpClientTestSupport {

    @Test(timeout = 60000)
    public void testCreateQueueSender() throws Exception {
        AmqpClient client = createAmqpClient();

        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getQueues().length);

        AmqpSender sender = session.createSender("queue://" + getTestName());

        assertEquals(1, brokerService.getAdminView().getQueues().length);
        assertNotNull(getProxyToQueue(getTestName()));
        assertEquals(1, brokerService.getAdminView().getQueueProducers().length);
        sender.close();
        assertEquals(0, brokerService.getAdminView().getQueueProducers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateTopicSender() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getTopics().length);

        AmqpSender sender = session.createSender("topic://" + getTestName());

        assertEquals(1, brokerService.getAdminView().getTopics().length);
        assertNotNull(getProxyToTopic(getTestName()));
        assertEquals(1, brokerService.getAdminView().getTopicProducers().length);
        sender.close();
        assertEquals(0, brokerService.getAdminView().getTopicProducers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSenderSettlementModeSettledIsHonored() throws Exception {
        doTestSenderSettlementModeIsHonored(SenderSettleMode.SETTLED);
    }

    @Test(timeout = 60000)
    public void testSenderSettlementModeUnsettledIsHonored() throws Exception {
        doTestSenderSettlementModeIsHonored(SenderSettleMode.UNSETTLED);
    }

    @Test(timeout = 60000)
    public void testSenderSettlementModeMixedIsHonored() throws Exception {
        doTestSenderSettlementModeIsHonored(SenderSettleMode.MIXED);
    }

    public void doTestSenderSettlementModeIsHonored(SenderSettleMode settleMode) throws Exception {
        AmqpClient client = createAmqpClient();

        client.setTraceFrames(true);

        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getQueues().length);

        AmqpSender sender = session.createSender(
            "queue://" + getTestName(), settleMode, ReceiverSettleMode.FIRST);

        assertEquals(1, brokerService.getAdminView().getQueues().length);
        assertNotNull(getProxyToQueue(getTestName()));
        assertEquals(1, brokerService.getAdminView().getQueueProducers().length);

        assertEquals(settleMode, sender.getEndpoint().getRemoteSenderSettleMode());

        AmqpMessage message = new AmqpMessage();
        message.setText("Test-Message");
        sender.send(message);

        sender.close();
        assertEquals(0, brokerService.getAdminView().getQueueProducers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiverSettlementModeSetToFirst() throws Exception {
        doTestReceiverSettlementModeForcedToFirst(ReceiverSettleMode.FIRST);
    }

    @Test(timeout = 60000)
    public void testReceiverSettlementModeSetToSecond() throws Exception {
        doTestReceiverSettlementModeForcedToFirst(ReceiverSettleMode.SECOND);
    }

    /*
     * The Broker does not currently support ReceiverSettleMode of SECOND so we ensure that
     * it always drops that back to FIRST to let the client know.  The client will need to
     * check and react accordingly.
     */
    private void doTestReceiverSettlementModeForcedToFirst(ReceiverSettleMode modeToUse) throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getQueues().length);

        AmqpSender sender = session.createSender(
            "queue://" + getTestName(), SenderSettleMode.UNSETTLED, modeToUse);

        assertEquals(1, brokerService.getAdminView().getQueues().length);
        assertNotNull(getProxyToQueue(getTestName()));
        assertEquals(1, brokerService.getAdminView().getQueueProducers().length);

        assertEquals(ReceiverSettleMode.FIRST, sender.getEndpoint().getRemoteReceiverSettleMode());

        sender.close();
        assertEquals(0, brokerService.getAdminView().getQueueProducers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendMessageToQueue() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        AmqpMessage message = new AmqpMessage();

        message.setText("Test-Message");

        sender.send(message);

        QueueViewMBean queue = getProxyToQueue(getTestName());

        assertEquals(1, queue.getQueueSize());

        sender.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendMultipleMessagesToQueue() throws Exception {
        final int MSG_COUNT = 100;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());

        for (int i = 0; i < MSG_COUNT; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message: " + i);
            sender.send(message);
        }

        QueueViewMBean queue = getProxyToQueue(getTestName());

        assertEquals(MSG_COUNT, queue.getQueueSize());

        sender.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testUnsettledSender() throws Exception {
        final int MSG_COUNT = 1000;

        final CountDownLatch settled = new CountDownLatch(MSG_COUNT);

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());

        connection.setStateInspector(new AmqpValidator() {

            @Override
            public void inspectDeliveryUpdate(Sender sender, Delivery delivery) {
                if (delivery.remotelySettled()) {
                    LOG.trace("Remote settled message for sender: {}", sender.getName());
                    settled.countDown();
                }
            }
        });

        AmqpSession session = connection.createSession();
        AmqpSender sender = session.createSender("topic://" + getTestName(), false);

        for (int i = 1; i <= MSG_COUNT; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message: " + i);
            sender.send(message);

            if (i % 1000 == 0) {
                LOG.info("Sent message: {}", i);
            }
        }

        final TopicViewMBean topic = getProxyToTopic(getTestName());
        assertTrue("All messages should arrive", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return topic.getEnqueueCount() == MSG_COUNT;
            }
        }));

        sender.close();

        assertTrue("Remote should have settled all deliveries", settled.await(5, TimeUnit.MINUTES));

        connection.close();
    }

    @Test(timeout = 60000)
    public void testPresettledSender() throws Exception {
        final int MSG_COUNT = 1000;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("topic://" + getTestName(), true);

        for (int i = 1; i <= MSG_COUNT; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setText("Test-Message: " + i);
            sender.send(message);

            if (i % 1000 == 0) {
                LOG.info("Sent message: {}", i);
            }
        }

        final TopicViewMBean topic = getProxyToTopic(getTestName());
        assertTrue("All messages should arrive", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return topic.getEnqueueCount() == MSG_COUNT;
            }
        }));

        sender.close();
        connection.close();
    }

    @Test
    public void testDeliveryDelayOfferedWhenRequested() throws Exception {

        final BrokerViewMBean brokerView = getProxyToBroker();

        AmqpClient client = createAmqpClient();
        client.setValidator(new AmqpValidator() {

            @Override
            public void inspectOpenedResource(Sender sender) {

                Symbol[] offered = sender.getRemoteOfferedCapabilities();
                if (!contains(offered, AmqpSupport.DELAYED_DELIVERY)) {
                    markAsInvalid("Broker did not indicate it support delayed message delivery");
                }
            }
        });

        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerView.getQueues().length);

        AmqpSender sender = session.createSender("queue://" + getTestName(), new Symbol[] { AmqpSupport.DELAYED_DELIVERY });
        assertNotNull(sender);

        assertEquals(1, brokerView.getQueues().length);

        connection.getStateInspector().assertValid();

        sender.close();
        connection.close();
    }
}