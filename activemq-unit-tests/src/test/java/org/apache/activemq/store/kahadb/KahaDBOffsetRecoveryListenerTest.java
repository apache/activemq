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
package org.apache.activemq.store.kahadb;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryContext;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import static org.junit.Assert.assertEquals;

public class KahaDBOffsetRecoveryListenerTest {

    private static final Logger logger = LoggerFactory.getLogger(KahaDBOffsetRecoveryListenerTest.class);

    protected BrokerService brokerService = null;
    protected BrokerService restartBrokerService = null;

    @Rule
    public TestName testName = new TestName();

    protected final int PRETEST_MSG_COUNT = 7531;

    @Before
    public void beforeEach() throws Exception {
        // Send+Recv a odd number of messages beyond cache sizes
        // to confirm the queue's sequence number gets pushed off
        sendMessages(PRETEST_MSG_COUNT, testName.getMethodName(), true);
        assertEquals(Integer.valueOf(PRETEST_MSG_COUNT), Integer.valueOf(receiveMessages(testName.getMethodName())));
    }

    @After
    public void afterEach() {
        brokerService = null;
        restartBrokerService = null;
    }

    protected BrokerService createBroker(KahaDBStore kaha) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistenceAdapter(kaha);
        broker.start();
        broker.waitUntilStarted(10_000L);
        return broker;
    }

    private KahaDBStore createStore(boolean delete) throws IOException {
        KahaDBStore kaha = new KahaDBStore();
        kaha.setJournalMaxFileLength(1024*100);
        kaha.setDirectory(new File(IOHelper.getDefaultDataDirectory(), "kahadb-recovery-tests"));
        if( delete ) {
            kaha.deleteAllMessages();
        }
        return kaha;
    }

    protected void runOffsetTest(final int sendCount, final int expectedMessageCount, final int recoverOffset, final int recoverCount, final int expectedRecoverCount, final int expectedRecoverIndex, final String queueName) throws Exception {
        runOffsetLoopTest(sendCount, expectedMessageCount, recoverOffset, recoverCount, expectedRecoverCount, expectedRecoverIndex, queueName, 1, false);
    }

    protected void runOffsetLoopTest(final int sendCount, final int expectedMessageCount, final int recoverOffset, final int recoverCount, final int expectedRecoverCount, final int expectedRecoverIndex, final String queueName, final int loopCount, final boolean repeatExpected) throws Exception {
        KahaDBStore kahaDBStore = createStore(true);
        brokerService = createBroker(kahaDBStore);
        sendMessages(sendCount, queueName, false);
        MessageStore messageStore = kahaDBStore.createQueueMessageStore(new ActiveMQQueue(queueName));

        int tmpExpectedRecoverCount = expectedRecoverCount;
        int tmpExpectedRecoverIndex = expectedRecoverIndex;
        int tmpRecoverOffset = recoverOffset;

        for(int i=0; i<loopCount; i++) {
            logger.info("Loop:{} recoverOffset:{} expectedRecoverCount:{} expectedRecoverIndex:{}", loopCount, tmpRecoverOffset, tmpExpectedRecoverCount, tmpExpectedRecoverIndex);

            TestMessageRecoveryListener testMessageRecoveryListener = new TestMessageRecoveryListener();
            assertEquals(Integer.valueOf(expectedMessageCount), Integer.valueOf(messageStore.getMessageCount()));

            messageStore.recoverMessages(new MessageRecoveryContext.Builder()
                    .messageRecoveryListener(testMessageRecoveryListener)
                    .offset(tmpRecoverOffset)
                    .maxMessageCountReturned(recoverCount).build());

            assertEquals(Integer.valueOf(tmpExpectedRecoverCount), Integer.valueOf(testMessageRecoveryListener.getRecoveredMessages().size()));

            if(tmpExpectedRecoverIndex >= 0) {
                assertEquals(Integer.valueOf(tmpExpectedRecoverIndex), (Integer)testMessageRecoveryListener.getRecoveredMessages().get(0).getProperty("index"));
            }

            if(!repeatExpected) {
                int nextExpectedRecoverCount = calculateExpectedRecoverCount(tmpRecoverOffset, tmpExpectedRecoverCount, expectedMessageCount);
                int nextExpectedRecoverIndex = calculateExpectedRecoverIndex(tmpRecoverOffset, tmpExpectedRecoverCount, tmpExpectedRecoverIndex, expectedMessageCount);
                int nextRecoverOffset = calculateRecoverOffset(tmpRecoverOffset, recoverCount, expectedMessageCount);
    
                tmpExpectedRecoverCount = nextExpectedRecoverCount;
                tmpExpectedRecoverIndex = nextExpectedRecoverIndex;
                tmpRecoverOffset = nextRecoverOffset;
            }
        }

        brokerService.stop();
        brokerService.waitUntilStopped();

        restartBrokerService = createBroker(createStore(false));
        restartBrokerService.start();
        restartBrokerService.waitUntilStarted(30_000L);
        assertEquals(sendCount, receiveMessages(queueName));

        restartBrokerService.stop();
        restartBrokerService.waitUntilStopped();
    }

    @Test
    public void testOffsetZero() throws Exception {
        runOffsetTest(1_000, 1_000, 0, 1, 1, 0, testName.getMethodName());
    }

    @Test
    public void testOffsetOne() throws Exception {
        runOffsetTest(1_000, 1_000, 1, 1, 1, 1, testName.getMethodName());
    }

    @Test
    public void testOffsetLastMinusOne() throws Exception {
        runOffsetTest(1_000, 1_000, 999, 1, 1, 999, testName.getMethodName());
    }

    @Test
    public void testOffsetLast() throws Exception {
        runOffsetTest(1_000, 1_000, 1_000, 1, 0, -1, testName.getMethodName());
    }

    @Test
    public void testOffsetBeyondQueueSizeNoError() throws Exception {
        runOffsetTest(1_000, 1_000, 10_000, 1, 0, -1, testName.getMethodName());
    }

    @Test
    public void testOffsetEmptyQueue() throws Exception {
        runOffsetTest(0, 0, 10_000, 1, 0, -1, testName.getMethodName());
    }

    @Test
    public void testOffsetWalk() throws Exception {
        runOffsetLoopTest(10_000, 10_000, 9_000, 200, 200, 9_000, testName.getMethodName(), 8, false);
    }

    @Test
    public void testOffsetRepeat() throws Exception {
        runOffsetLoopTest(10_000, 10_000, 7_000, 133, 133, 7_000, testName.getMethodName(), 10, true);
    }

    private void sendMessages(int count, String queueName, boolean sendAsync) throws JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        if(sendAsync) {
            cf.setProducerWindowSize(800000);
            cf.setUseAsyncSend(true);
        }
        cf.setWatchTopicAdvisories(false);

        Connection connection = cf.createConnection();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(new ActiveMQQueue(queueName));
            for (int i = 0; i < count; i++) {
                TextMessage textMessage = session.createTextMessage(createContent(i));
                textMessage.setIntProperty("index", i);
                producer.send(textMessage);
            }
        } finally {
            connection.close();
        }
    }

    private int receiveMessages(String queueName) throws JMSException {
        int rc=0;
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        cf.setWatchTopicAdvisories(false);

        Connection connection = cf.createConnection();

        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(new ActiveMQQueue(queueName));
            while (messageConsumer.receive(1_000L) != null) {
                rc++;
            }
            return rc;
        } finally {
            connection.close();
        }
    }

    private String createContent(int i) {
        StringBuilder sb = new StringBuilder(i+":");
        while( sb.length() < 1024 ) {
            sb.append("*");
        }
        return sb.toString();
    }

    private static int calculateExpectedRecoverCount(final int recoverOffset, final int expectedRecoverCount, final int expectedMessageCount) {
        int nextOffset = calculateRecoverOffset(recoverOffset, expectedRecoverCount, expectedMessageCount);
        if(nextOffset >= expectedMessageCount) {
            return 0;
        }

        int nextRange = nextOffset + expectedRecoverCount;
        int remaining = expectedMessageCount - nextRange;

        if(remaining <= 0) {
            return expectedRecoverCount - remaining;
        }

        return expectedRecoverCount;
    }

    private static int calculateExpectedRecoverIndex(final int recoverOffset, final int expectedRecoverCount, final int expectedRecoverIndex, final int expectedMessageCount) {
        int nextOffset = calculateRecoverOffset(recoverOffset, expectedRecoverCount, expectedMessageCount);

        if(nextOffset >= (expectedMessageCount - 1)) {
            return -1;
        }

        return nextOffset;
    }

    private static int calculateRecoverOffset(final int recoverOffset, final int expectedRecoverCount, final int expectedMessageCount) {
        return (recoverOffset + expectedRecoverCount);
    }

    // int tmpRecoverOffset = recoverOffset;

    static class TestMessageRecoveryListener implements MessageRecoveryListener {

        List<MessageId> recoveredMessageIds = new LinkedList<>();
        List<Message> recoveredMessages = new LinkedList<>();

        @Override
        public boolean hasSpace() {
            return true;
        }

        @Override
        public boolean isDuplicate(MessageId messageId) {
            return recoveredMessageIds.contains(messageId);
        }

        @Override
        public boolean recoverMessage(Message message) throws Exception {
            if(recoveredMessages.contains(message)) {
                return false;
            }
            return recoveredMessages.add(message);
        }

        @Override
        public boolean recoverMessageReference(MessageId messageId) throws Exception {
            if(recoveredMessageIds.contains(messageId)) {
                return false;
            }
            return recoveredMessageIds.add(messageId);
        }

        public List<MessageId> getRecoveredMessageIds() {
            return recoveredMessageIds;
        }

        public List<Message> getRecoveredMessages() {
            return recoveredMessages;
        }
    }
}
