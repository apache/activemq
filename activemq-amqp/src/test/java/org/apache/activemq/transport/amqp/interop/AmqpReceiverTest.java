/*
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

import static org.apache.activemq.transport.amqp.AmqpSupport.JMS_SELECTOR_FILTER_IDS;
import static org.apache.activemq.transport.amqp.AmqpSupport.NO_LOCAL_FILTER_IDS;
import static org.apache.activemq.transport.amqp.AmqpSupport.findFilter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.junit.ActiveMQTestRunner;
import org.apache.activemq.junit.Repeat;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.transport.amqp.client.AmqpUnknownFilterType;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.activemq.util.Wait;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test various behaviors of AMQP receivers with the broker.
 */
@RunWith(ActiveMQTestRunner.class)
public class AmqpReceiverTest extends AmqpClientTestSupport {

    @Override
    protected boolean isUseOpenWireConnector() {
        return true;
    }

    @Test(timeout = 60000)
    public void testReceiverCloseSendsRemoteClose() throws Exception {
        AmqpClient client = createAmqpClient();
        assertNotNull(client);

        final AtomicBoolean closed = new AtomicBoolean();

        client.setValidator(new AmqpValidator() {

            @Override
            public void inspectClosedResource(Session session) {
                LOG.info("Session closed: {}", session.getContext());
            }

            @Override
            public void inspectDetachedResource(Receiver receiver) {
                markAsInvalid("Broker should not detach receiver linked to closed session.");
            }

            @Override
            public void inspectClosedResource(Receiver receiver) {
                LOG.info("Receiver closed: {}", receiver.getContext());
                closed.set(true);
            }
        });

        AmqpConnection connection = trackConnection(client.connect());
        assertNotNull(connection);
        AmqpSession session = connection.createSession();
        assertNotNull(session);
        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());
        assertNotNull(receiver);

        receiver.close();

        assertTrue("Did not process remote close as expected", closed.get());
        connection.getStateInspector().assertValid();

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateQueueReceiver() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getQueues().length);

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());

        assertEquals(1, brokerService.getAdminView().getQueues().length);
        assertNotNull(getProxyToQueue(getTestName()));
        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);
        receiver.close();
        assertEquals(0, brokerService.getAdminView().getQueueSubscribers().length);

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
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getQueues().length);

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName(), settleMode, ReceiverSettleMode.FIRST);

        assertEquals(1, brokerService.getAdminView().getQueues().length);
        assertNotNull(getProxyToQueue(getTestName()));
        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);

        assertEquals(settleMode, receiver.getEndpoint().getRemoteSenderSettleMode());

        receiver.close();
        assertEquals(0, brokerService.getAdminView().getQueueSubscribers().length);

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

        AmqpReceiver receiver = session.createReceiver(
            "queue://" + getTestName(), SenderSettleMode.MIXED, modeToUse);

        assertEquals(1, brokerService.getAdminView().getQueues().length);
        assertNotNull(getProxyToQueue(getTestName()));
        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);

        assertEquals(ReceiverSettleMode.FIRST, receiver.getEndpoint().getRemoteReceiverSettleMode());

        receiver.close();
        assertEquals(0, brokerService.getAdminView().getQueueSubscribers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateQueueReceiverWithJMSSelector() throws Exception {
        AmqpClient client = createAmqpClient();

        client.setValidator(new AmqpValidator() {

            @SuppressWarnings("unchecked")
            @Override
            public void inspectOpenedResource(Receiver receiver) {
                LOG.info("Receiver opened: {}", receiver);

                if (receiver.getRemoteSource() == null) {
                    markAsInvalid("Link opened with null source.");
                }

                Source source = (Source) receiver.getRemoteSource();
                Map<Symbol, Object> filters = source.getFilter();

                if (findFilter(filters, JMS_SELECTOR_FILTER_IDS) == null) {
                    markAsInvalid("Broker did not return the JMS Filter on Attach");
                }
            }
        });

        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getQueues().length);

        session.createReceiver("queue://" + getTestName(), "JMSPriority > 8");

        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);

        connection.getStateInspector().assertValid();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateQueueReceiverWithNoLocalSet() throws Exception {
        AmqpClient client = createAmqpClient();

        client.setValidator(new AmqpValidator() {

            @SuppressWarnings("unchecked")
            @Override
            public void inspectOpenedResource(Receiver receiver) {
                LOG.info("Receiver opened: {}", receiver);

                if (receiver.getRemoteSource() == null) {
                    markAsInvalid("Link opened with null source.");
                }

                Source source = (Source) receiver.getRemoteSource();
                Map<Symbol, Object> filters = source.getFilter();

                if (findFilter(filters, NO_LOCAL_FILTER_IDS) == null) {
                    markAsInvalid("Broker did not return the NoLocal Filter on Attach");
                }
            }
        });

        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getQueues().length);

        session.createReceiver("queue://" + getTestName(), null, true);

        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);

        connection.getStateInspector().assertValid();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testCreateTopicReceiver() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getTopics().length);

        AmqpReceiver receiver = session.createReceiver("topic://" + getTestName());

        assertEquals(1, brokerService.getAdminView().getTopics().length);
        assertNotNull(getProxyToTopic(getTestName()));
        assertEquals(1, brokerService.getAdminView().getTopicSubscribers().length);
        receiver.close();
        assertEquals(0, brokerService.getAdminView().getTopicSubscribers().length);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testQueueReceiverReadMessage() throws Exception {
        sendMessages(getTestName(), 1, false);

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());

        QueueViewMBean queueView = getProxyToQueue(getTestName());
        assertEquals(1, queueView.getQueueSize());
        assertEquals(0, queueView.getDispatchCount());

        receiver.flow(1);
        assertNotNull(receiver.receive(5, TimeUnit.SECONDS));
        receiver.close();

        assertEquals(1, queueView.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    @Repeat(repetitions = 1)
    public void testPresettledReceiverReadsAllMessages() throws Exception {
        final int MSG_COUNT = 100;
        sendMessages(getTestName(), MSG_COUNT, false);

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName(), null, false, true);

        QueueViewMBean queueView = getProxyToQueue(getTestName());
        assertEquals(MSG_COUNT, queueView.getQueueSize());
        assertEquals(0, queueView.getDispatchCount());

        receiver.flow(MSG_COUNT);
        for (int i = 0; i < MSG_COUNT; ++i) {
            assertNotNull(receiver.receive(5, TimeUnit.SECONDS));
        }
        receiver.close();

        assertEquals(0, queueView.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    @Repeat(repetitions = 1)
    public void testPresettledReceiverReadsAllMessagesInNonFlowBatchQueue() throws Exception {
        doTestPresettledReceiverReadsAllMessagesInNonFlowBatch(false);
    }

    @Test(timeout = 60000)
    @Repeat(repetitions = 1)
    public void testPresettledReceiverReadsAllMessagesInNonFlowBatchTopic() throws Exception {
        doTestPresettledReceiverReadsAllMessagesInNonFlowBatch(true);
    }

    private void doTestPresettledReceiverReadsAllMessagesInNonFlowBatch(boolean topic) throws Exception {

        final String destinationName;
        if (topic) {
            destinationName = "topic://" + getTestName();
        } else {
            destinationName = "queue://" + getTestName();
        }

        final int MSG_COUNT = 100;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver = session.createReceiver(destinationName, null, false, true);

        sendMessages(getTestName(), MSG_COUNT, topic);

        final DestinationViewMBean destinationView;
        if (topic) {
            destinationView = getProxyToTopic(getTestName());
        } else {
            destinationView = getProxyToQueue(getTestName());
        }
        assertEquals(MSG_COUNT, destinationView.getEnqueueCount());
        assertEquals(0, destinationView.getDispatchCount());

        receiver.flow(20);
        // consume less that flow
        for (int j=0;j<10;j++) {
            assertNotNull(receiver.receive(5, TimeUnit.SECONDS));
        }

        // flow more and consume all
        receiver.flow(10);
        for (int j=0;j<20;j++) {
            assertNotNull(receiver.receive(5, TimeUnit.SECONDS));
        }

        // remainder
        receiver.flow(70);
        for (int j=0;j<70;j++) {
            assertNotNull(receiver.receive(5, TimeUnit.SECONDS));
        }

        receiver.close();

        assertEquals(0, destinationView.getEnqueueCount() - destinationView.getDequeueCount());

        connection.close();
    }

    @Test(timeout = 60000)
    @Repeat(repetitions = 1)
    public void testTwoQueueReceiversOnSameConnectionReadMessagesNoDispositions() throws Exception {
        int MSG_COUNT = 4;
        sendMessages(getTestName(), MSG_COUNT, false);

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver1 = session.createReceiver("queue://" + getTestName());

        QueueViewMBean queueView = getProxyToQueue(getTestName());
        assertEquals(MSG_COUNT, queueView.getQueueSize());

        receiver1.flow(2);
        assertNotNull(receiver1.receive(5, TimeUnit.SECONDS));
        assertNotNull(receiver1.receive(5, TimeUnit.SECONDS));

        AmqpReceiver receiver2 = session.createReceiver("queue://" + getTestName());

        assertEquals(2, brokerService.getAdminView().getQueueSubscribers().length);

        receiver2.flow(2);
        assertNotNull(receiver2.receive(5, TimeUnit.SECONDS));
        assertNotNull(receiver2.receive(5, TimeUnit.SECONDS));

        assertEquals(MSG_COUNT, queueView.getDispatchCount());
        assertEquals(0, queueView.getDequeueCount());

        receiver1.close();
        receiver2.close();

        assertEquals(MSG_COUNT, queueView.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testTwoQueueReceiversOnSameConnectionReadMessagesAcceptOnEach() throws Exception {
        int MSG_COUNT = 4;
        sendMessages(getTestName(), MSG_COUNT, false);

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver1 = session.createReceiver("queue://" + getTestName());

        final QueueViewMBean queueView = getProxyToQueue(getTestName());
        assertEquals(MSG_COUNT, queueView.getQueueSize());

        receiver1.flow(2);
        AmqpMessage message = receiver1.receive(5, TimeUnit.SECONDS);
        assertNotNull(message);
        message.accept();
        message = receiver1.receive(5, TimeUnit.SECONDS);
        assertNotNull(message);
        message.accept();

        assertTrue("Should have ack'd two", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getDequeueCount() == 2;
            }
        }, TimeUnit.SECONDS.toMillis(5), TimeUnit.MILLISECONDS.toMillis(50)));

        AmqpReceiver receiver2 = session.createReceiver("queue://" + getTestName());

        assertEquals(2, brokerService.getAdminView().getQueueSubscribers().length);

        receiver2.flow(2);
        message = receiver2.receive(5, TimeUnit.SECONDS);
        assertNotNull(message);
        message.accept();
        message = receiver2.receive(5, TimeUnit.SECONDS);
        assertNotNull(message);
        message.accept();

        assertEquals(MSG_COUNT, queueView.getDispatchCount());
        assertTrue("Queue should be empty now", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getDequeueCount() == 4;
            }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(10)));

        receiver1.close();
        receiver2.close();

        assertEquals(0, queueView.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSecondReceiverOnQueueGetsAllUnconsumedMessages() throws Exception {
        int MSG_COUNT = 20;
        sendMessages(getTestName(), MSG_COUNT, false);

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver1 = session.createReceiver("queue://" + getTestName());

        final QueueViewMBean queueView = getProxyToQueue(getTestName());
        assertEquals(MSG_COUNT, queueView.getQueueSize());

        receiver1.flow(20);

        assertTrue("Should have dispatch to prefetch", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getInFlightCount() >= 2;
            }
        }, TimeUnit.SECONDS.toMillis(5), TimeUnit.MILLISECONDS.toMillis(50)));

        receiver1.close();

        AmqpReceiver receiver2 = session.createReceiver("queue://" + getTestName());

        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);

        receiver2.flow(MSG_COUNT * 2);
        AmqpMessage message = receiver2.receive(5, TimeUnit.SECONDS);
        assertNotNull(message);
        message.accept();
        message = receiver2.receive(5, TimeUnit.SECONDS);
        assertNotNull(message);
        message.accept();

        assertTrue("Should have ack'd two", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return queueView.getDequeueCount() == 2;
            }
        }, TimeUnit.SECONDS.toMillis(5), TimeUnit.MILLISECONDS.toMillis(50)));

        receiver2.close();

        assertEquals(MSG_COUNT - 2, queueView.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testUnsupportedFiltersAreNotListedAsSupported() throws Exception {
        AmqpClient client = createAmqpClient();

        client.setValidator(new AmqpValidator() {

            @SuppressWarnings("unchecked")
            @Override
            public void inspectOpenedResource(Receiver receiver) {
                LOG.info("Receiver opened: {}", receiver);

                if (receiver.getRemoteSource() == null) {
                    markAsInvalid("Link opened with null source.");
                }

                Source source = (Source) receiver.getRemoteSource();
                Map<Symbol, Object> filters = source.getFilter();

                if (findFilter(filters, AmqpUnknownFilterType.UNKNOWN_FILTER_IDS) != null) {
                    markAsInvalid("Broker should not return unsupported filter on attach.");
                }
            }
        });

        Map<Symbol, DescribedType> filters = new HashMap<>();
        filters.put(AmqpUnknownFilterType.UNKNOWN_FILTER_NAME, AmqpUnknownFilterType.UNKOWN_FILTER);

        Source source = new Source();
        source.setAddress("queue://" + getTestName());
        source.setFilter(filters);
        source.setDurable(TerminusDurability.NONE);
        source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        assertEquals(0, brokerService.getAdminView().getQueues().length);

        session.createReceiver(source);

        assertEquals(1, brokerService.getAdminView().getQueueSubscribers().length);

        connection.getStateInspector().assertValid();
        connection.close();
    }

    @Test(timeout = 30000)
    public void testReleasedDisposition() throws Exception {
        sendMessages(getTestName(), 1, false);

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver = session.createReceiver(getTestName());
        receiver.flow(2);

        AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull("did not receive message first time", message);

        Message protonMessage = message.getWrappedMessage();
        assertNotNull(protonMessage);
        assertEquals("Unexpected initial value for AMQP delivery-count", 0, protonMessage.getDeliveryCount());

        message.release();

        // Read the message again and validate its state

        message = receiver.receive(10, TimeUnit.SECONDS);
        assertNotNull("did not receive message again", message);

        message.accept();

        protonMessage = message.getWrappedMessage();
        assertNotNull(protonMessage);
        assertEquals("Unexpected updated value for AMQP delivery-count", 0, protonMessage.getDeliveryCount());

        connection.close();
    }

    @Test(timeout = 30000)
    public void testRejectedDisposition() throws Exception {
        sendMessages(getTestName(), 1, false);

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver = session.createReceiver(getTestName());
        receiver.flow(2);

        AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull("did not receive message first time", message);

        Message protonMessage = message.getWrappedMessage();
        assertNotNull(protonMessage);
        assertEquals("Unexpected initial value for AMQP delivery-count", 0, protonMessage.getDeliveryCount());

        message.reject();

        // Attempt to read the message again but should not get it.
        message = receiver.receive(2, TimeUnit.SECONDS);
        assertNull("shoudl not receive message again", message);

        connection.close();
    }

    @Test(timeout = 30000)
    public void testModifiedDispositionWithDeliveryFailedWithoutUndeliverableHereFieldsSet() throws Exception {
        doModifiedDispositionTestImpl(Boolean.TRUE, null);
    }

    @Test(timeout = 30000)
    public void testModifiedDispositionWithoutDeliveryFailedWithoutUndeliverableHereFieldsSet() throws Exception {
        doModifiedDispositionTestImpl(null, null);
    }

    @Test(timeout = 30000)
    public void testModifiedDispositionWithoutDeliveryFailedWithUndeliverableHereFieldsSet() throws Exception {
        doModifiedDispositionTestImpl(null, Boolean.TRUE);
    }

    @Test(timeout = 30000)
    public void testModifiedDispositionWithDeliveryFailedWithUndeliverableHereFieldsSet() throws Exception {
        doModifiedDispositionTestImpl(Boolean.TRUE, Boolean.TRUE);
    }

    private void doModifiedDispositionTestImpl(Boolean deliveryFailed, Boolean undeliverableHere) throws Exception {
        int msgCount = 1;
        sendMessages(getTestName(), msgCount, false);

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());
        receiver.flow(2 * msgCount);

        AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull("did not receive message first time", message);

        Message protonMessage = message.getWrappedMessage();
        assertNotNull(protonMessage);
        assertEquals("Unexpected initial value for AMQP delivery-count", 0, protonMessage.getDeliveryCount());

        message.modified(deliveryFailed, undeliverableHere);

        if(Boolean.TRUE.equals(undeliverableHere)) {
            message = receiver.receive(250, TimeUnit.MILLISECONDS);
            assertNull("Should not receive message again", message);
        } else {
            message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull("did not receive message again", message);

            int expectedDeliveryCount = 0;
            if(Boolean.TRUE.equals(deliveryFailed)) {
                expectedDeliveryCount = 1;
            }

            message.accept();

            Message protonMessage2 = message.getWrappedMessage();
            assertNotNull(protonMessage2);
            assertEquals("Unexpected updated value for AMQP delivery-count", expectedDeliveryCount, protonMessage2.getDeliveryCount());
        }

        receiver.close();
        connection.close();
    }
}
