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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.DeliveryMode;
import javax.jms.Queue;
import javax.jms.Topic;

import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.junit.ActiveMQTestRunner;
import org.apache.activemq.junit.Repeat;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.util.Wait;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic send and receive scenarios using only AMQP sender and receiver
 * links.
 */
@RunWith(ActiveMQTestRunner.class)
public class AmqpSendReceiveTest extends AmqpClientTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(AmqpSendReceiveTest.class);

    private static final int PAYLOAD = 110 * 1024;

    @Test(timeout = 60000)
    public void testSimpleSendOneReceiveOneToQueue() throws Exception {
        doTestSimpleSendOneReceiveOne(Queue.class);
    }

    @Test(timeout = 60000)
    public void testSimpleSendOneReceiveOneToTopic() throws Exception {
        doTestSimpleSendOneReceiveOne(Topic.class);
    }

    public void doTestSimpleSendOneReceiveOne(Class<?> destType) throws Exception {

        final String address;
        if (Queue.class.equals(destType)) {
            address = "queue://" + getTestName();
        } else {
            address = "topic://" + getTestName();
        }

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender(address);
        AmqpReceiver receiver = session.createReceiver(address);

        AmqpMessage message = new AmqpMessage();

        message.setMessageId("msg" + 1);
        message.setMessageAnnotation("serialNo", 1);
        message.setText("Test-Message");

        sender.send(message);
        sender.close();

        LOG.info("Attempting to read message with receiver");
        receiver.flow(2);
        AmqpMessage received = receiver.receive(10, TimeUnit.SECONDS);
        assertNotNull("Should have read message", received);
        assertEquals("msg1", received.getMessageId());
        received.accept();

        receiver.close();

        connection.close();
    }

    @Test(timeout = 60000)
    public void testCloseBusyReceiver() throws Exception {
        final int MSG_COUNT = 20;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());

        for (int i = 0; i < MSG_COUNT; i++) {
            AmqpMessage message = new AmqpMessage();

            message.setMessageId("msg" + i);
            message.setMessageAnnotation("serialNo", i);
            message.setText("Test-Message");

            sender.send(message);
        }

        sender.close();

        QueueViewMBean queue = getProxyToQueue(getTestName());
        assertEquals(20, queue.getQueueSize());

        AmqpReceiver receiver1 = session.createReceiver("queue://" + getTestName());
        receiver1.flow(MSG_COUNT);
        AmqpMessage received = receiver1.receive(5, TimeUnit.SECONDS);
        assertNotNull("Should have got a message", received);
        assertEquals("msg0", received.getMessageId());
        receiver1.close();

        AmqpReceiver receiver2 = session.createReceiver("queue://" + getTestName());
        receiver2.flow(200);
        for (int i = 0; i < MSG_COUNT; ++i) {
            received = receiver2.receive(5, TimeUnit.SECONDS);
            assertNotNull("Should have got a message", received);
            assertEquals("msg" + i, received.getMessageId());
        }

        receiver2.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiveWithJMSSelectorFilter() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpMessage message1 = new AmqpMessage();
        message1.setGroupId("abcdefg");
        message1.setApplicationProperty("sn", 100);

        AmqpMessage message2 = new AmqpMessage();
        message2.setGroupId("hijklm");
        message2.setApplicationProperty("sn", 200);

        AmqpSender sender = session.createSender("queue://" + getTestName());
        sender.send(message1);
        sender.send(message2);
        sender.close();

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName(), "sn = 100");
        receiver.flow(2);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull("Should have read a message", received);
        assertEquals(100, received.getApplicationProperty("sn"));
        assertEquals("abcdefg", received.getGroupId());
        received.accept();

        assertNull(receiver.receive(1, TimeUnit.SECONDS));

        receiver.close();
        connection.close();
    }

    @Test(timeout = 60000)
    @Repeat(repetitions = 1)
    public void testAdvancedLinkFlowControl() throws Exception {
        final int MSG_COUNT = 20;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());

        for (int i = 0; i < MSG_COUNT; i++) {
            AmqpMessage message = new AmqpMessage();

            message.setMessageId("msg" + i);
            message.setMessageAnnotation("serialNo", i);
            message.setText("Test-Message");

            sender.send(message);
        }

        sender.close();

        LOG.info("Attempting to read first two messages with receiver #1");
        AmqpReceiver receiver1 = session.createReceiver("queue://" + getTestName());
        receiver1.flow(2);
        AmqpMessage message1 = receiver1.receive(10, TimeUnit.SECONDS);
        AmqpMessage message2 = receiver1.receive(10, TimeUnit.SECONDS);
        assertNotNull("Should have read message 1", message1);
        assertNotNull("Should have read message 2", message2);
        assertEquals("msg0", message1.getMessageId());
        assertEquals("msg1", message2.getMessageId());
        message1.accept();
        message2.accept();

        LOG.info("Attempting to read next two messages with receiver #2");
        AmqpReceiver receiver2 = session.createReceiver("queue://" + getTestName());
        receiver2.flow(2);
        AmqpMessage message3 = receiver2.receive(10, TimeUnit.SECONDS);
        AmqpMessage message4 = receiver2.receive(10, TimeUnit.SECONDS);
        assertNotNull("Should have read message 3", message3);
        assertNotNull("Should have read message 4", message4);
        assertEquals("msg2", message3.getMessageId());
        assertEquals("msg3", message4.getMessageId());
        message3.accept();
        message4.accept();

        LOG.info("Attempting to read remaining messages with receiver #1");
        receiver1.flow(MSG_COUNT - 4);
        for (int i = 4; i < MSG_COUNT; i++) {
            AmqpMessage message = receiver1.receive(10, TimeUnit.SECONDS);
            assertNotNull("Should have read a message", message);
            assertEquals("msg" + i, message.getMessageId());
            message.accept();
        }

        receiver1.close();
        receiver2.close();

        connection.close();
    }

    @Test(timeout = 60000)
    @Repeat(repetitions = 1)
    public void testDispatchOrderWithPrefetchOfOne() throws Exception {
        final int MSG_COUNT = 20;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());

        for (int i = 0; i < MSG_COUNT; i++) {
            AmqpMessage message = new AmqpMessage();

            message.setMessageId("msg" + i);
            message.setMessageAnnotation("serialNo", i);
            message.setText("Test-Message");

            sender.send(message);
        }

        sender.close();

        AmqpReceiver receiver1 = session.createReceiver("queue://" + getTestName());
        receiver1.flow(1);

        AmqpReceiver receiver2 = session.createReceiver("queue://" + getTestName());
        receiver2.flow(1);

        AmqpMessage message1 = receiver1.receive(10, TimeUnit.SECONDS);
        AmqpMessage message2 = receiver2.receive(10, TimeUnit.SECONDS);
        assertNotNull("Should have read message 1", message1);
        assertNotNull("Should have read message 2", message2);
        assertEquals("msg0", message1.getMessageId());
        assertEquals("msg1", message2.getMessageId());
        message1.accept();
        message2.accept();

        receiver1.flow(1);
        AmqpMessage message3 = receiver1.receive(10, TimeUnit.SECONDS);
        receiver2.flow(1);
        AmqpMessage message4 = receiver2.receive(10, TimeUnit.SECONDS);
        assertNotNull("Should have read message 3", message3);
        assertNotNull("Should have read message 4", message4);
        assertEquals("msg2", message3.getMessageId());
        assertEquals("msg3", message4.getMessageId());
        message3.accept();
        message4.accept();

        LOG.info("*** Attempting to read remaining messages with both receivers");
        int splitCredit = (MSG_COUNT - 4) / 2;

        LOG.info("**** Receiver #1 granting creadit[{}] for its block of messages", splitCredit);
        receiver1.flow(splitCredit);
        for (int i = 0; i < splitCredit; i++) {
            AmqpMessage message = receiver1.receive(10, TimeUnit.SECONDS);
            assertNotNull("Receiver #1 should have read a message", message);
            LOG.info("Receiver #1 read message: {}", message.getMessageId());
            message.accept();
        }

        LOG.info("**** Receiver #2 granting creadit[{}] for its block of messages", splitCredit);
        receiver2.flow(splitCredit);
        for (int i = 0; i < splitCredit; i++) {
            AmqpMessage message = receiver2.receive(10, TimeUnit.SECONDS);
            assertNotNull("Receiver #2 should have read message[" + i + "]", message);
            LOG.info("Receiver #2 read message: {}", message.getMessageId());
            message.accept();
        }

        receiver1.close();
        receiver2.close();

        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiveMessageAndRefillCreditBeforeAcceptOnQueue() throws Exception {
        doTestReceiveMessageAndRefillCreditBeforeAccept(Queue.class);
    }

    @Test(timeout = 60000)
    public void testReceiveMessageAndRefillCreditBeforeAcceptOnTopic() throws Exception {
        doTestReceiveMessageAndRefillCreditBeforeAccept(Topic.class);
    }

    private void doTestReceiveMessageAndRefillCreditBeforeAccept(Class<?> destType) throws Exception {

        AmqpClient client = createAmqpClient();

        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        final String address;
        if (Queue.class.equals(destType)) {
            address = "queue://" + getTestName();
        } else {
            address = "topic://" + getTestName();
        }

        AmqpReceiver receiver = session.createReceiver(address);
        AmqpSender sender = session.createSender(address);

        for (int i = 0; i < 2; i++) {
            AmqpMessage message = new AmqpMessage();
            message.setMessageId("msg" + i);
            sender.send(message);
        }
        sender.close();

        receiver.flow(1);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull(received);

        receiver.flow(1);
        received.accept();

        received = receiver.receive(10, TimeUnit.SECONDS);
        assertNotNull(received);
        received.accept();

        receiver.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiveMessageAndRefillCreditBeforeAcceptOnQueueAsync() throws Exception {
        doTestReceiveMessageAndRefillCreditBeforeAcceptOnTopicAsync(Queue.class);
    }

    @Test(timeout = 60000)
    public void testReceiveMessageAndRefillCreditBeforeAcceptOnTopicAsync() throws Exception {
        doTestReceiveMessageAndRefillCreditBeforeAcceptOnTopicAsync(Topic.class);
    }

    private void doTestReceiveMessageAndRefillCreditBeforeAcceptOnTopicAsync(Class<?> destType) throws Exception {
        final AmqpClient client = createAmqpClient();
        final LinkedList<Throwable> errors = new LinkedList<>();
        final CountDownLatch receiverReady = new CountDownLatch(1);
        ExecutorService executorService = Executors.newCachedThreadPool();

        final String address;
        if (Queue.class.equals(destType)) {
            address = "queue://" + getTestName();
        } else {
            address = "topic://" + getTestName();
        }

        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.info("Starting consumer connection");
                    AmqpConnection connection = trackConnection(client.connect());
                    AmqpSession session = connection.createSession();
                    AmqpReceiver receiver = session.createReceiver(address);
                    receiver.flow(1);
                    receiverReady.countDown();
                    AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
                    assertNotNull(received);

                    receiver.flow(1);
                    received.accept();

                    received = receiver.receive(5, TimeUnit.SECONDS);
                    assertNotNull(received);
                    received.accept();

                    receiver.close();
                    connection.close();

                } catch (Exception error) {
                    errors.add(error);
                }
            }
        });

        // producer
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    receiverReady.await(20, TimeUnit.SECONDS);
                    AmqpConnection connection = trackConnection(client.connect());
                    AmqpSession session = connection.createSession();

                    AmqpSender sender = session.createSender(address);
                    for (int i = 0; i < 2; i++) {
                        AmqpMessage message = new AmqpMessage();
                        message.setMessageId("msg" + i);
                        sender.send(message);
                    }
                    sender.close();
                    connection.close();
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }
            }
        });

        executorService.shutdown();
        executorService.awaitTermination(20, TimeUnit.SECONDS);
        assertTrue("no errors: " + errors, errors.isEmpty());
    }

    @Test(timeout = 60000)
    public void testMessageDurabliltyFollowsSpec() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        AmqpReceiver receiver1 = session.createReceiver("queue://" + getTestName());

        QueueViewMBean queue = getProxyToQueue(getTestName());

        // Create default message that should be sent as non-durable
        AmqpMessage message1 = new AmqpMessage();
        message1.setText("Test-Message -> non-durable");
        message1.setDurable(false);
        message1.setMessageId("ID:Message:1");
        sender.send(message1);

        assertEquals(1, queue.getQueueSize());
        receiver1.flow(1);
        message1 = receiver1.receive(50, TimeUnit.SECONDS);
        assertNotNull("Should have read a message", message1);
        assertFalse("First message sent should not be durable", message1.isDurable());
        message1.accept();

        // Create default message that should be sent as non-durable
        AmqpMessage message2 = new AmqpMessage();
        message2.setText("Test-Message -> durable");
        message2.setDurable(true);
        message2.setMessageId("ID:Message:2");
        sender.send(message2);

        assertEquals(1, queue.getQueueSize());
        receiver1.flow(1);
        message2 = receiver1.receive(50, TimeUnit.SECONDS);
        assertNotNull("Should have read a message", message2);
        assertTrue("Second message sent should be durable", message2.isDurable());
        message2.accept();

        sender.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testMessageWithNoHeaderNotMarkedDurable() throws Exception {
        doMessageNotMarkedDurableTestImpl(false, false);
    }

    @Test(timeout = 60000)
    public void testMessageWithHeaderAndDefaultedNonDurableNotMarkedDurable() throws Exception {
        doMessageNotMarkedDurableTestImpl(true, false);
    }

    @Test(timeout = 60000)
    public void testMessageWithHeaderAndMarkedNonDurableNotMarkedDurable() throws Exception {
        doMessageNotMarkedDurableTestImpl(true, true);
    }

    private void doMessageNotMarkedDurableTestImpl(boolean sendHeaderWithPriority, boolean explicitSetNonDurable) throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        AmqpReceiver receiver1 = session.createReceiver("queue://" + getTestName());

        Message protonMessage = Message.Factory.create();
        protonMessage.setMessageId("ID:Message:1");
        protonMessage.setBody(new AmqpValue("Test-Message -> non-durable"));
        if(sendHeaderWithPriority) {
            Header header = new Header();
            if(explicitSetNonDurable) {
                header.setDurable(false);
            }
            header.setPriority(UnsignedByte.valueOf((byte) 5));

            protonMessage.setHeader(header);
        } else {
            assertNull("Should not have a header", protonMessage.getHeader());
        }

        AmqpMessage message1 = new AmqpMessage(protonMessage);

        sender.send(message1);

        final QueueViewMBean queueView = getProxyToQueue(getTestName());
        assertNotNull(queueView);

        assertEquals(1, queueView.getQueueSize());

        List<javax.jms.Message> messages = (List<javax.jms.Message>) queueView.browseMessages();
        assertEquals(1, messages.size());
        javax.jms.Message queueMessage = messages.get(0);
        assertEquals("Queued message should not be persistent", DeliveryMode.NON_PERSISTENT, queueMessage.getJMSDeliveryMode());

        receiver1.flow(1);
        AmqpMessage message2 = receiver1.receive(50, TimeUnit.SECONDS);
        assertNotNull("Should have read a message", message2);
        assertFalse("Received message should not be durable", message2.isDurable());
        message2.accept();

        sender.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendMessageToQueueNoPrefixReceiveWithPrefix() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender(getTestName());
        AmqpMessage message = new AmqpMessage();

        message.setText("Test-Message");

        sender.send(message);

        QueueViewMBean queueView = getProxyToQueue(getTestName());
        assertEquals(1, queueView.getQueueSize());
        sender.close();

        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());

        assertEquals(1, queueView.getQueueSize());
        assertEquals(0, queueView.getDispatchCount());

        receiver.flow(1);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull(received);
        received.accept();
        receiver.close();

        assertEquals(0, queueView.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendMessageToQueueWithPrefixReceiveWithNoPrefix() throws Exception {
        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        AmqpMessage message = new AmqpMessage();

        message.setText("Test-Message");

        sender.send(message);

        QueueViewMBean queueView = getProxyToQueue(getTestName());
        assertEquals(1, queueView.getQueueSize());
        sender.close();

        AmqpReceiver receiver = session.createReceiver(getTestName());

        assertEquals(1, queueView.getQueueSize());
        assertEquals(0, queueView.getDispatchCount());

        receiver.flow(1);
        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
        assertNotNull(received);
        received.accept();
        receiver.close();

        assertEquals(0, queueView.getQueueSize());

        connection.close();
    }

    @Test(timeout = 60000)
    public void testReceiveMessageBeyondAckedAmountQueue() throws Exception {
        doTestReceiveMessageBeyondAckedAmount(Queue.class);
    }

    @Test(timeout = 60000)
    public void testReceiveMessageBeyondAckedAmountTopic() throws Exception {
        doTestReceiveMessageBeyondAckedAmount(Topic.class);
    }

    private void doTestReceiveMessageBeyondAckedAmount(Class<?> destType) throws Exception {
        final int MSG_COUNT = 50;

        AmqpClient client = createAmqpClient();

        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        final String address;
        if (Queue.class.equals(destType)) {
            address = "queue://" + getTestName();
        } else {
            address = "topic://" + getTestName();
        }

        AmqpReceiver receiver = session.createReceiver(address);
        AmqpSender sender = session.createSender(address);

        final DestinationViewMBean destinationView;
        if (Queue.class.equals(destType)) {
            destinationView = getProxyToQueue(getTestName());
        } else {
            destinationView = getProxyToTopic(getTestName());
        }

        for (int i = 0; i < MSG_COUNT; i++) {
            AmqpMessage message = new AmqpMessage();
            message.setMessageId("msg" + i);
            sender.send(message);
        }

        List<AmqpMessage> pendingAcks = new ArrayList<>();

        for (int i = 0; i < MSG_COUNT; i++) {
            receiver.flow(1);
            AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull(received);
            pendingAcks.add(received);
        }

        // Send one more to check in-flight stays at zero with no credit and all
        // pending messages settled.
        AmqpMessage message = new AmqpMessage();
        message.setMessageId("msg-final");
        sender.send(message);

        for (AmqpMessage pendingAck : pendingAcks) {
            pendingAck.accept();
        }

        assertTrue("Should be no inflight messages: " + destinationView.getInFlightCount(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return destinationView.getInFlightCount() == 0;
            }
        }));

        sender.close();
        receiver.close();
        connection.close();
    }

    @Test(timeout = 60000)
    public void testTwoPresettledReceiversReceiveAllMessages() throws Exception {
        final int MSG_COUNT = 100;

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        final String address = "queue://" + getTestName();

        AmqpSender sender = session.createSender(address);
        AmqpReceiver receiver1 = session.createReceiver(address, null, false, true);
        AmqpReceiver receiver2 = session.createReceiver(address, null, false, true);

        for (int i = 0; i < MSG_COUNT; i++) {
            AmqpMessage message = new AmqpMessage();
            message.setMessageId("msg" + i);
            sender.send(message);
        }

        final DestinationViewMBean destinationView = getProxyToQueue(getTestName());

        LOG.info("Attempting to read first two messages with receiver #1");
        receiver1.flow(2);
        AmqpMessage message1 = receiver1.receive(10, TimeUnit.SECONDS);
        AmqpMessage message2 = receiver1.receive(10, TimeUnit.SECONDS);
        assertNotNull("Should have read message 1", message1);
        assertNotNull("Should have read message 2", message2);
        assertEquals("msg0", message1.getMessageId());
        assertEquals("msg1", message2.getMessageId());
        message1.accept();
        message2.accept();

        LOG.info("Attempting to read next two messages with receiver #2");
        receiver2.flow(2);
        AmqpMessage message3 = receiver2.receive(10, TimeUnit.SECONDS);
        AmqpMessage message4 = receiver2.receive(10, TimeUnit.SECONDS);
        assertNotNull("Should have read message 3", message3);
        assertNotNull("Should have read message 4", message4);
        assertEquals("msg2", message3.getMessageId());
        assertEquals("msg3", message4.getMessageId());
        message3.accept();
        message4.accept();

        assertTrue("Should be no inflight messages: " + destinationView.getInFlightCount(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return destinationView.getInFlightCount() == 0;
            }
        }));

        LOG.info("*** Attempting to read remaining messages with both receivers");
        int splitCredit = (MSG_COUNT - 4) / 2;

        LOG.info("**** Receiver #1 granting credit[{}] for its block of messages", splitCredit);
        receiver1.flow(splitCredit);
        for (int i = 0; i < splitCredit; i++) {
            AmqpMessage message = receiver1.receive(10, TimeUnit.SECONDS);
            assertNotNull("Receiver #1 should have read a message", message);
            LOG.info("Receiver #1 read message: {}", message.getMessageId());
            message.accept();
        }

        LOG.info("**** Receiver #2 granting credit[{}] for its block of messages", splitCredit);
        receiver2.flow(splitCredit);
        for (int i = 0; i < splitCredit; i++) {
            AmqpMessage message = receiver2.receive(10, TimeUnit.SECONDS);
            assertNotNull("Receiver #2 should have read a message[" + i + "]", message);
            LOG.info("Receiver #2 read message: {}", message.getMessageId());
            message.accept();
        }

        receiver1.close();
        receiver2.close();

        connection.close();
    }

    @Test(timeout = 60000)
    public void testSendReceiveLotsOfDurableMessagesOnQueue() throws Exception {
        doTestSendReceiveLotsOfDurableMessages(Queue.class);
    }

    @Test(timeout = 60000)
    public void testSendReceiveLotsOfDurableMessagesOnTopic() throws Exception {
        doTestSendReceiveLotsOfDurableMessages(Topic.class);
    }

    private void doTestSendReceiveLotsOfDurableMessages(Class<?> destType) throws Exception {
        final int MSG_COUNT = 1000;

        AmqpClient client = createAmqpClient();

        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        final CountDownLatch done = new CountDownLatch(MSG_COUNT);
        final AtomicBoolean error = new AtomicBoolean(false);
        final ExecutorService executor = Executors.newSingleThreadExecutor();

        final String address;
        if (Queue.class.equals(destType)) {
            address = "queue://" + getTestName();
        } else {
            address = "topic://" + getTestName();
        }

        final AmqpReceiver receiver = session.createReceiver(address);
        receiver.flow(MSG_COUNT);

        AmqpSender sender = session.createSender(address);

        final DestinationViewMBean destinationView;
        if (Queue.class.equals(destType)) {
            destinationView = getProxyToQueue(getTestName());
        } else {
            destinationView = getProxyToTopic(getTestName());
        }

        executor.execute(new Runnable() {

            @Override
            public void run() {
                for (int i = 0; i < MSG_COUNT; i++) {
                    try {
                        AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
                        received.accept();
                        done.countDown();
                    } catch (Exception ex) {
                        LOG.info("Caught error: {}", ex.getClass().getSimpleName());
                        error.set(true);
                    }
                }
            }
        });

        for (int i = 0; i < MSG_COUNT; i++) {
            AmqpMessage message = new AmqpMessage();
            message.setMessageId("msg" + i);
            sender.send(message);
        }

        assertTrue("did not read all messages, waiting on: " + done.getCount(), done.await(10, TimeUnit.SECONDS));
        assertFalse("should not be any errors on receive", error.get());

        assertTrue("Should be no inflight messages: " + destinationView.getInFlightCount(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return destinationView.getInFlightCount() == 0;
            }
        }));

        sender.close();
        receiver.close();
        connection.close();
    }

   @Test(timeout = 60000)
   public void testSendAMQPMessageWithComplexAnnotationsReceiveAMQP() throws Exception {
      String testQueueName = "ConnectionFrameSize";
      int nMsgs = 200;

      AmqpClient client = createAmqpClient();

      Symbol annotation = Symbol.valueOf("x-opt-embedded-map");
      Map<String, String> embeddedMap = new LinkedHashMap<>();
      embeddedMap.put("test-key-1", "value-1");
      embeddedMap.put("test-key-2", "value-2");
      embeddedMap.put("test-key-3", "value-3");

      {
         AmqpConnection connection = client.connect();
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(testQueueName);
         AmqpMessage message = createAmqpMessage((byte) 'A', PAYLOAD);

         message.setApplicationProperty("IntProperty", 42);
         message.setDurable(true);
         message.setMessageAnnotation(annotation.toString(), embeddedMap);
         sender.send(message);
         session.close();
         connection.close();
      }

      {
         AmqpConnection connection = client.connect();
         AmqpSession session = connection.createSession();
         AmqpReceiver receiver = session.createReceiver(testQueueName);
         receiver.flow(nMsgs);

         AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull("Failed to read message with embedded map in annotations", message);
         MessageImpl wrapped = (MessageImpl) message.getWrappedMessage();
         if (wrapped.getBody() instanceof Data) {
            Data data = (Data) wrapped.getBody();
            System.out.println("received : message: " + data.getValue().getLength());
            assertEquals(PAYLOAD, data.getValue().getLength());
         }

         assertNotNull(message.getWrappedMessage().getMessageAnnotations());
         assertNotNull(message.getWrappedMessage().getMessageAnnotations().getValue());
         assertEquals(embeddedMap, message.getWrappedMessage().getMessageAnnotations().getValue().get(annotation));

         message.accept();
         session.close();
         connection.close();
      }
   }

   private AmqpMessage createAmqpMessage(byte value, int payloadSize) {
       AmqpMessage message = new AmqpMessage();
       byte[] payload = new byte[payloadSize];
       for (int i = 0; i < payload.length; i++) {
          payload[i] = value;
       }
       message.setBytes(payload);
       return message;
    }
}
