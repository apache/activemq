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
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import static org.junit.Assert.assertEquals;

public class KahaDBOffsetRecoveryListenerTest {

    protected BrokerService brokerService = null;
    protected KahaDBStore kaha = null;

    @Before
    public void beforeEach() throws Exception {

    }

    @After
    public void afterEach() {
        brokerService = null;
        kaha = null;
    }

    protected BrokerService createBroker(KahaDBStore kaha) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistenceAdapter(kaha);
        broker.start();
        broker.waitUntilStarted(10_000l);
        return broker;
    }

    private KahaDBStore createStore(boolean delete) throws IOException {
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File("target/activemq-data/kahadb-recovery-tests"));
        if( delete ) {
            kaha.deleteAllMessages();
        }
        return kaha;
    }

    protected void runOffsetTest(int sendCount, int expectedMessageCount, int recoverOffset, int recoverCount, int expectedRecoverCount, int expectedRecoverIndex, String queueName) throws Exception {
        kaha = createStore(true);
        kaha.setJournalMaxFileLength(1024*100);
        brokerService = createBroker(kaha);
        sendMessages(sendCount, queueName);
        brokerService.stop();
        brokerService.waitUntilStopped();

        TestMessageRecoveryListener testMessageRecoveryListener = new TestMessageRecoveryListener();
        kaha = createStore(false);
        kaha.start();
        MessageStore messageStore = kaha.createQueueMessageStore(new ActiveMQQueue(queueName));
        messageStore.start();
        assertEquals(Integer.valueOf(expectedMessageCount), Integer.valueOf(messageStore.getMessageCount()));
        messageStore.recoverNextMessages(recoverOffset, recoverCount, testMessageRecoveryListener);
        messageStore.stop();
        kaha.stop();

        assertEquals(Integer.valueOf(expectedRecoverCount), Integer.valueOf(testMessageRecoveryListener.getRecoveredMessages().size()));

        if(expectedRecoverIndex >= 0) {
            assertEquals(Integer.valueOf(expectedRecoverIndex), (Integer)testMessageRecoveryListener.getRecoveredMessages().get(0).getProperty("index"));
        }

        brokerService = createBroker(kaha);
        assertEquals(sendCount, receiveMessages(queueName));
    }

    @Test
    public void testOffsetZero() throws Exception {
        runOffsetTest(1_000, 1_000, 0, 1, 1, 0, "TEST.OFFSET.ZERO");
    }

    @Test
    public void testOffsetOne() throws Exception {
        runOffsetTest(1_000, 1_000, 1, 1, 1, 1, "TEST.OFFSET.ONE");
    }

    @Test
    public void testOffsetLastMinusOne() throws Exception {
        runOffsetTest(1_000, 1_000, 999, 1, 1, 999, "TEST.OFFSET.LASTMINUSONE");
    }

    @Test
    public void testOffsetLast() throws Exception {
        runOffsetTest(1_000, 1_000, 1_000, 1, 0, -1, "TEST.OFFSET.LAST");
    }

    @Test
    public void testOffsetBeyondQueueSizeNoError() throws Exception {
        runOffsetTest(1_000, 1_000, 10_000, 1, 0, -1, "TEST.OFFSET.BEYOND");
    }

    @Test
    public void testOffsetEmptyQueue() throws Exception {
        runOffsetTest(0, 0, 10_000, 1, 0, -1, "TEST.OFFSET.EMPTY");
    }

    private void sendMessages(int count, String queueName) throws JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        cf.setUseAsyncSend(true);
        cf.setProducerWindowSize(1024);
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
        Connection connection = cf.createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(new ActiveMQQueue(queueName));
            while ( messageConsumer.receive(1000) !=null ) {
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
